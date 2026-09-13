// `sqlx::migrate!` embeds the migrations at compile time and cargo cannot see that it read them, so
// without this a new migration leaves a cached build running the old schema.
fn main() {
    println!("cargo:rerun-if-changed=../../migrations");
}
