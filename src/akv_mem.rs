use libactionkv::ActionKV;

#[cfg(target_os = "windows")]
const uSAGE: &str = "
    Usage:
        akv_mem.exe FILE get KEY
        akv_mem.exe FILE delete KEY
        akv_mem.exe FILE insert KEY VALUE
        akv_mem.exe FILE update KEY VALUE
";

#[cfg(not(target_os = "windows"))]
const USAGE: &str = "
    Usage:
        akv_mem FILE get KEY
        akv_mem FILE delete KEY
        akv_mem FILE insert KEY VALUE
        akv_mem FILE update KEY VALUE
";

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let fname = args.get(1).expect(&USAGE);
    let action: &str = args.get(2).expect(&USAGE).as_ref();
    let key: &str = args.get(3).expect(&USAGE).as_ref();
    let maybe_value = args.get(4);

    let path = std::path::Path::new(fname);
    let mut store = ActionKV::open(path).expect("Unable to open file");
    store.load().expect("unable to load data");

    match action {
        "get" => match store.get(key.as_bytes()).unwrap() {
            None => eprintln!("{:?} not found", key),
            Some(value) => println!("{:?}", String::from_utf8_lossy(value.as_slice())),
        },
        "delete" => store.delete(key.as_bytes()).unwrap(),
        "insert" => {
            let value: &str = maybe_value.expect("&USAGE").as_ref();
            store.insert(key.as_bytes(), value.as_bytes()).unwrap()
        }
        "update" => {
            let value: &str = maybe_value.expect("&USAGE").as_ref();
            store.update(key.as_bytes(), value.as_bytes()).unwrap()
        }

        _ => eprintln!("{}", &USAGE),
    }
}
