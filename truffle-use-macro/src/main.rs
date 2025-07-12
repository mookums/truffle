use truffle_macro::query;

fn main() {
    query!("select id from user");
}
