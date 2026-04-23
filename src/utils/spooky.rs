const SPOOKY_SEED_64: u64 = 0x972c4bdc_u64;
const BASE_C: u64 = 0xdeadbeefdeadbeef_u64;
const D_CONST: u64 = BASE_C.wrapping_add(8 << 56);

#[inline]
const fn rot64(x: u64, k: u32) -> u64 {
    x.rotate_left(k)
}

#[inline]
fn short_mix(h0: &mut u64, h1: &mut u64, h2: &mut u64, h3: &mut u64) {
    *h2 = rot64(*h2, 50);
    *h2 = h2.wrapping_add(*h3);
    *h0 ^= *h2;
    *h3 = rot64(*h3, 52);
    *h3 = h3.wrapping_add(*h0);
    *h1 ^= *h3;
    *h0 = rot64(*h0, 30);
    *h0 = h0.wrapping_add(*h1);
    *h2 ^= *h0;
    *h1 = rot64(*h1, 41);
    *h1 = h1.wrapping_add(*h2);
    *h3 ^= *h1;
    *h2 = rot64(*h2, 54);
    *h2 = h2.wrapping_add(*h3);
    *h0 ^= *h2;
    *h3 = rot64(*h3, 48);
    *h3 = h3.wrapping_add(*h0);
    *h1 ^= *h3;
    *h0 = rot64(*h0, 38);
    *h0 = h0.wrapping_add(*h1);
    *h2 ^= *h0;
    *h1 = rot64(*h1, 37);
    *h1 = h1.wrapping_add(*h2);
    *h3 ^= *h1;
    *h2 = rot64(*h2, 62);
    *h2 = h2.wrapping_add(*h3);
    *h0 ^= *h2;
    *h3 = rot64(*h3, 34);
    *h3 = h3.wrapping_add(*h0);
    *h1 ^= *h3;
    *h0 = rot64(*h0, 5);
    *h0 = h0.wrapping_add(*h1);
    *h2 ^= *h0;
    *h1 = rot64(*h1, 36);
    *h1 = h1.wrapping_add(*h2);
    *h3 ^= *h1;
}

#[inline]
fn short_end(h0: &mut u64, h1: &mut u64, h2: &mut u64, h3: &mut u64) {
    *h3 ^= *h2;
    *h2 = rot64(*h2, 15);
    *h3 = h3.wrapping_add(*h2);
    *h0 ^= *h3;
    *h3 = rot64(*h3, 52);
    *h0 = h0.wrapping_add(*h3);
    *h1 ^= *h0;
    *h0 = rot64(*h0, 26);
    *h1 = h1.wrapping_add(*h0);
    *h2 ^= *h1;
    *h1 = rot64(*h1, 51);
    *h2 = h2.wrapping_add(*h1);
    *h3 ^= *h2;
    *h2 = rot64(*h2, 28);
    *h3 = h3.wrapping_add(*h2);
    *h0 ^= *h3;
    *h3 = rot64(*h3, 9);
    *h0 = h0.wrapping_add(*h3);
    *h1 ^= *h0;
    *h0 = rot64(*h0, 47);
    *h1 = h1.wrapping_add(*h0);
    *h2 ^= *h1;
    *h1 = rot64(*h1, 54);
    *h2 = h2.wrapping_add(*h1);
    *h3 ^= *h2;
    *h2 = rot64(*h2, 32);
    *h3 = h3.wrapping_add(*h2);
    *h0 ^= *h3;
    *h3 = rot64(*h3, 25);
    *h0 = h0.wrapping_add(*h3);
    *h1 ^= *h0;
    *h0 = rot64(*h0, 63);
    *h1 = h1.wrapping_add(*h0);
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

pub fn spooky_hash_pair(a: u64, b: u64) -> u64 {
    let mut h0 = SPOOKY_SEED_64;
    let mut h1 = SPOOKY_SEED_64;
    let mut h2 = BASE_C.wrapping_add(a);
    let mut h3 = BASE_C.wrapping_add(b);

    short_mix(&mut h0, &mut h1, &mut h2, &mut h3);

    h2 = h2.wrapping_add(BASE_C);
    h3 = h3.wrapping_add(BASE_C.wrapping_add(16_u64 << 56));
    short_end(&mut h0, &mut h1, &mut h2, &mut h3);
    h0
}
