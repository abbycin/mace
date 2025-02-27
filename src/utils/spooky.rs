const SPOOKY_SEED_64: u64 = 0x972c4bdc_u64;
const BASE_C: u64 = 0xdeadbeefdeadbeef_u64;
const D_CONST: u64 = BASE_C.wrapping_add(8 << 56);

#[inline]
const fn rot64(x: u64, k: u32) -> u64 {
    (x << k) | (x >> (64 - k))
}

pub fn spooky_hash(key: u64) -> u64 {
    let (mut h0, mut h1, mut h2, mut h3) = (
        SPOOKY_SEED_64,
        SPOOKY_SEED_64,
        BASE_C.wrapping_add(key),
        D_CONST,
    );

    h3 ^= h2;
    h2 = rot64(h2, 15);
    h3 = h3.wrapping_add(h2);
    h0 ^= h3;
    h3 = rot64(h3, 52);
    h0 = h0.wrapping_add(h3);
    h1 ^= h0;
    h0 = rot64(h0, 26);
    h1 = h1.wrapping_add(h0);
    h2 ^= h1;
    h1 = rot64(h1, 51);
    h2 = h2.wrapping_add(h1);
    h3 ^= h2;
    h2 = rot64(h2, 28);
    h3 = h3.wrapping_add(h2);
    h0 ^= h3;
    h3 = rot64(h3, 9);
    h0 = h0.wrapping_add(h3);
    h1 ^= h0;
    h0 = rot64(h0, 47);
    h1 = h1.wrapping_add(h0);
    h2 ^= h1;
    h1 = rot64(h1, 54);
    h2 = h2.wrapping_add(h1);
    h3 ^= h2;
    h2 = rot64(h2, 32);
    h3 = h3.wrapping_add(h2);
    h0 ^= h3;
    h3 = rot64(h3, 25);
    h0 = h0.wrapping_add(h3);
    h0 = rot64(h0, 63);

    h0
}
