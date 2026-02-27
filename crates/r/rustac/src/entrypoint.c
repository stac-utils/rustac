// We need to forward routine registration from C to Rust
// to avoid the linker removing the bytes from the final library.
// See <https://github.com/pola-rs/r-polars/issues/1292> for more details.
void R_init_rustac_extendr(void *dll);

void R_init_rustac(void *dll) {
  R_init_rustac_extendr(dll);
}
