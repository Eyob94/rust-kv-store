use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    io::{self, BufReader, BufWriter, Read as _, Seek, SeekFrom, Write},
    path::Path,
};

use byteorder::{LittleEndian, ReadBytesExt as _, WriteBytesExt as _};

type ByteString = Vec<u8>;
type ByteStr = [u8];

pub struct KeyValuePair {
    pub key: ByteString,
    pub value: ByteString,
}

#[derive(Debug)]
pub struct ActionKV {
    file: File,
    pub index: HashMap<ByteString, u64>,
}

const CRC32: crc::Crc<u32> = crc::Crc::<u32>::new(&crc::CRC_32_CKSUM);
impl ActionKV {
    pub fn open(path: &Path) -> io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .append(true)
            .open(path)?;
        let index = HashMap::new();

        Ok(ActionKV { file, index })
    }

    fn process_record<R: io::Read>(file: &mut R) -> io::Result<KeyValuePair> {
        let saved_checksum = file.read_u32::<LittleEndian>()?;
        let key_len = file.read_u32::<LittleEndian>()?;
        let val_len = file.read_u32::<LittleEndian>()?;
        let data_len = key_len + val_len;

        let mut data = ByteString::with_capacity(data_len as usize);

        {
            file.by_ref().take(data_len as u64).read_to_end(&mut data)?;
        }

        debug_assert_eq!(data.len(), data_len as usize);

        let checksum = CRC32.checksum(&data);

        if checksum != saved_checksum {
            panic!(
                "data corruption encountered ({:08x} != {:08x})",
                checksum, saved_checksum
            )
        }

        let value = data.split_off(key_len as usize);
        let key = data;

        Ok(KeyValuePair { key, value })
    }

    pub fn insert(&mut self, key: &ByteStr, value: &ByteStr) -> io::Result<()> {
        let position = self.insert_but_ignore_index(key, value)?;

        self.index.insert(key.to_vec(), position);
        Ok(())
    }

    pub fn update(&mut self, key: &ByteStr, value: &ByteStr) -> io::Result<()> {
        self.insert(key, value)
    }

    pub fn delete(&mut self, key:&ByteStr)->io::Result<()>{
        self.insert(key, b"")
    }

    pub fn get(&mut self, key: &ByteStr) -> io::Result<Option<ByteString>> {
        let position = match self.index.get(key) {
            Some(position) => *position,
            None => return Ok(None),
        };

        let kv = self.get_at(position)?;

        Ok(Some(kv.value))
    }

    pub fn get_at(&mut self, position: u64) -> io::Result<KeyValuePair> {
        let mut buf = BufReader::new(&mut self.file);
        buf.seek(SeekFrom::Start(position))?;
        let kv = Self::process_record(&mut buf)?;

        Ok(kv)
    }

    pub fn insert_but_ignore_index(&mut self, key: &ByteStr, value: &ByteStr) -> io::Result<u64> {
        let mut buf = BufWriter::new(&mut self.file);

        let key_len = key.len();
        let val_len = value.len();
        let mut tmp = ByteString::with_capacity(key_len + val_len);

        for byte in key {
            tmp.push(*byte);
        }

        for byte in value {
            tmp.push(*byte);
        }

        let checksum = CRC32.checksum(&tmp);

        let next_byte = SeekFrom::End(0);
        let current_position = buf.seek(SeekFrom::Current(0))?;
        buf.seek(next_byte)?;

        buf.write_u32::<LittleEndian>(checksum)?;
        buf.write_u32::<LittleEndian>(key_len as u32)?;
        buf.write_u32::<LittleEndian>(val_len as u32)?;
        buf.write_all(&tmp)?;

        Ok(current_position)
    }

    pub fn load(&mut self) -> io::Result<()> {
        let mut buffer_from_file = BufReader::new(&mut self.file);

        loop {
            let position = buffer_from_file.seek(SeekFrom::Current(0))?;

            let maybe_kv = Self::process_record(&mut buffer_from_file);

            let kv = match maybe_kv {
                Ok(kv) => kv,
                Err(err) => match err.kind() {
                    io::ErrorKind::UnexpectedEof => {
                        break;
                    }
                    _ => return Err(err),
                },
            };

            self.index.insert(kv.key, position);
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {

    #[test]
    fn it_works() {}
}
