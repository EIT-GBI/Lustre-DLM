# Single-pass, schema-specific fast path for the lines written by
# pipelines/stat.py.
#
# include()d by ProcessStat.jl: `handle_line!` calls `fastparse` first and
# falls back to the JSON.jl based `handle_line_json!` whenever it returns ok =
# false.
#
# stat.py emits each record through qpipe's json codec, i.e. Python
# ````
# json.dumps(rec, separators=(",", ":"))   # ensure_ascii=True, allow_nan=True
# ````
# with the keys in insertion order name, path, code[, st_size, st_atime,
# st_mtime, st_ctime].
# A code-0 line therefore looks exactly like
#     {"name":"x","path":"/a/x","code":0,"st_size":4096,"st_atime":1789914709.0,"st_mtime":...,"st_ctime":...}
#
# `fastparse` accepts ONLY that shape, with
#   * strings made of printable ASCII (0x20..0x7f) and no '"' or '\\' (no
#     escapes at all),
#   * st_size = 0 | [1-9][0-9]{0,17} (fits Int64, no sign),
#   * st_*time = -?(0|[1-9][0-9]*) '.' [0-9]+  (no exponent, no NaN/Infinity,
#     <= 64 bytes),
#   * nothing before '{' and nothing after '}' (process_chunk already trimmed
#     trailing ' ' / '\r').
# Floats: if the literal has <= 19 digits and, after dropping trailing fraction
# zeros, equals m * 10^-k with m <= 2^53 and k <= 22, then Float64(m) and
# 10.0^k are both exact and the single IEEE division Float64(m) / 10.0^k is the
# correctly rounded value (Clinger's fast path); JSON.jl (Parsers.parsefloat)
# is correctly rounded too, so the bits agree. All real stat.py timestamps
# (1781025120.0 etc.) take this path. Longer decimals go to Parsers.parsefloat,
# the same call JSON.jl makes for the token, so they agree by construction.
#
# The whole line is validated before anything is pushed, so a failure never
# leaves a partial row. Everything else -- escapes, non-ASCII bytes, other key
# order, missing / extra / duplicate keys, whitespace, code != 0, exponents,
# NaN/Infinity, big numbers, truncation -- returns ok=false and the caller
# falls back to the original JSON.jl based logic (handle_line_json!), so
# semantics are unchanged.

const _POW10 = ntuple(i -> 10.0^(i - 1), 23)  # 1e0 .. 1e22, all exact in Float64
const _MAXMANT = UInt64(1) << 53

const _LIT_NAME  = Tuple(codeunits("{\"name\":\""))
const _LIT_PATH  = Tuple(codeunits(",\"path\":\""))
const _LIT_CODE0 = Tuple(codeunits(",\"code\":0,\"st_size\":"))
const _LIT_ATIME = Tuple(codeunits(",\"st_atime\":"))
const _LIT_MTIME = Tuple(codeunits(",\"st_mtime\":"))
const _LIT_CTIME = Tuple(codeunits(",\"st_ctime\":"))

# Match the literal `lit` at data[p:...]; return the position after it, or 0.
@inline function _lit(
        data::Vector{UInt8}, p::Int, hi::Int, lit::NTuple{N,UInt8}
    ) where {N}
    p + N - 1 <= hi || return 0
    ok = true
    @inbounds for k in 1:N
        ok &= data[p + k - 1] == lit[k]
    end
    return ok ? p + N : 0
end

# String body starting at p (just after the opening quote). Returns the
# position of the closing quote, or 0 if there is none or the body holds a byte
# we do not take on the fast path ('\\', a control byte < 0x20, or a non-ASCII
# byte >= 0x80).
@inline function _str(data::Vector{UInt8}, p::Int, hi::Int)
    p <= hi || return 0
    q = GC.@preserve data ccall(
        :memchr, Ptr{UInt8}, (Ptr{UInt8}, Cint, Csize_t),
        pointer(data, p), Cint('"'), hi - p + 1
    )
    q == C_NULL && return 0
    e = p + Int(q - pointer(data, p))              # index of the closing '"'
    bad = false
    @inbounds @simd for i in p:e-1                 # branch-free, vectorises
        b = data[i]
        bad |= (b == 0x5c) | ((b - 0x20) >= 0x60)  # '\\', <0x20 (wraps) or >=0x80
    end
    return bad ? 0 : e
end

# Non-negative JSON integer of at most 18 digits at p. Returns (value, position
# after it) or (0, 0).
@inline function _uint(data::Vector{UInt8}, p::Int, hi::Int)
    p <= hi || return (0, 0)
    @inbounds b = data[p]
    d = b - UInt8('0')
    d <= 9 || return (0, 0)
    v = Int(d)
    p += 1
    if d == 0                                          # "0", but never "0123"
        p <= hi && (@inbounds data[p] - UInt8('0')) <= 9 && return (0, 0)
        return (0, p)
    end
    n = 1
    @inbounds while p <= hi
        d = data[p] - UInt8('0')
        d <= 9 || break
        v = 10v + Int(d)
        n += 1
        p += 1
    end
    n <= 18 || return (0, 0)                  # 18 digits always fit in Int64
    return (v, p)
end

# Tier 2 for plain decimals the exact fast path cannot take (more than 2^53 /
# 19 digits / 22 fraction digits, e.g. Python's 17-digit reprs of sub-second
# timestamps): call the very function JSON.jl 1.x itself uses on Parsers 3
# (`Parsers.parsefloat(Float64, bytes, first, last)`), via the Parsers module
# JSON.jl loaded, so the value is bit-identical by construction. On Parsers 2
# (other JSON.jl code path) we do not try to replicate it and the line falls
# back instead.
const _Parsers = JSON.Parsers
@static if !isdefined(_Parsers, :xparse2) && isdefined(_Parsers, :parsefloat)
    @inline function _slowfloat(data::Vector{UInt8}, a::Int, b::Int)
        v, rc = _Parsers.parsefloat(Float64, data, a, b)
        return (v, rc == _Parsers.RC_OK)
    end
else
    @inline _slowfloat(data::Vector{UInt8}, a::Int, b::Int) = (0.0, false)
end

# JSON number of the form -?(0|[1-9][0-9]*)\.[0-9]+ (no exponent) at p. Returns
# (value, next) or (0.0, 0) if it is not of that form.
@inline function _float(data::Vector{UInt8}, p::Int, hi::Int)
    p <= hi || return (0.0, 0)
    start = p
    neg = false
    @inbounds if data[p] == UInt8('-')
        neg = true
        p += 1
        p <= hi || return (0.0, 0)
    end
    m = UInt64(0)
    nd = 0                                   # digits accumulated into m
    @inbounds d = data[p] - UInt8('0')
    d <= 9 || return (0.0, 0)
    p += 1
    if d == 0
        p <= hi && (@inbounds data[p] - UInt8('0')) <= 9 && return (0.0, 0)  # leading zero
    else
        m = UInt64(d)
        nd = 1
        @inbounds while p <= hi
            d = data[p] - UInt8('0')
            d <= 9 || break
            m = 10m + d                      # may wrap; only used if nd <= 19
            nd += 1
            p += 1
        end
    end
    (p <= hi && (@inbounds data[p]) == UInt8('.')) || return (0.0, 0)  # need a fraction
    p += 1
    k = 0
    @inbounds while p <= hi
        d = data[p] - UInt8('0')
        d <= 9 || break
        m = 10m + d
        nd += 1
        k += 1
        p += 1
    end
    k >= 1 || return (0.0, 0)                # "1." is invalid JSON
    if p <= hi                               # no exponent on the fast path
        @inbounds b = data[p]
        (b == UInt8('e')) | (b == UInt8('E')) && return (0.0, 0)
    end
    if nd <= 19                              # m is exact (< 10^19 < 2^64)
        while k > 0 && m % 10 == 0           # 1789914709.0 -> m=1789914709, k=0
            m = div(m, 10)
            k -= 1
        end
        if (m <= _MAXMANT) & (k <= 22)
            v = Float64(m) / @inbounds(_POW10[k + 1])  # exact operands, one correctly rounded op
            return (neg ? -v : v, p)
        end
    end
    p - start <= 64 || return (0.0, 0)       # absurdly long literal: let JSON.jl decide
    v, ok = _slowfloat(data, start, p - 1)   # |v| < 1e64: no overflow / underflow
    return ok ? (v, p) : (0.0, 0)
end

# Parse one complete line data[lo:hi] (no '\n', trailing ' '/'\r' already
# trimmed). Returns a tuple; when `ok` is false the caller must use the generic
# JSON.jl path.
@inline function fastparse(data::Vector{UInt8}, lo::Int, hi::Int)
    fail = (false, 0, 0, 0, 0, 0, 0.0, 0.0, 0.0)
    p = _lit(data, lo, hi, _LIT_NAME);      p == 0 && return fail
    nlo = p
    q = _str(data, p, hi);                  q == 0 && return fail
    nhi = q - 1
    p = _lit(data, q + 1, hi, _LIT_PATH);   p == 0 && return fail
    plo = p
    q = _str(data, p, hi);                  q == 0 && return fail
    phi = q - 1
    p = _lit(data, q + 1, hi, _LIT_CODE0);  p == 0 && return fail
    sz, p = _uint(data, p, hi);             p == 0 && return fail
    p = _lit(data, p, hi, _LIT_ATIME);      p == 0 && return fail
    at, p = _float(data, p, hi);            p == 0 && return fail
    p = _lit(data, p, hi, _LIT_MTIME);      p == 0 && return fail
    mt, p = _float(data, p, hi);            p == 0 && return fail
    p = _lit(data, p, hi, _LIT_CTIME);      p == 0 && return fail
    ct, p = _float(data, p, hi);            p == 0 && return fail
    (p == hi && (@inbounds data[p]) == UInt8('}')) || return fail
    return (true, nlo, nhi, plo, phi, sz, at, mt, ct)
end
