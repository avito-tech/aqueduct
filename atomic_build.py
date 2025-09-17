import cffi

ffibuilder = cffi.FFI()

ffibuilder.cdef("""
uint32_t load_uint32(uint32_t *v);
void store_uint32(uint32_t *v, uint32_t n);
uint32_t add_and_fetch_uint32(uint32_t *v, uint32_t i);
uint32_t sub_and_fetch_uint32(uint32_t *v, uint32_t i);
""")

ffibuilder.set_source("aqueduct.shm._atomic", r"""
uint32_t load_uint32(uint32_t *v) {
    return __atomic_load_n(v, __ATOMIC_SEQ_CST);
};
void store_uint32(uint32_t *v, uint32_t n) {
    uint32_t i = n;
    __atomic_store(v, &i, __ATOMIC_SEQ_CST);
};
uint32_t add_and_fetch_uint32(uint32_t *v, uint32_t i) {
    return __atomic_add_fetch(v, i, __ATOMIC_SEQ_CST);
};
uint32_t sub_and_fetch_uint32(uint32_t *v, uint32_t i) {
    return __atomic_sub_fetch(v, i, __ATOMIC_SEQ_CST);
};
""")   # <=

if __name__ == "__main__":
    ffibuilder.compile(verbose=True)
