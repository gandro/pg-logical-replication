extern crate postgres_protocol;
extern crate postgres_shared;
extern crate bytes;

use std::io::{self, Write, Read, BufWriter};
use std::net::{TcpStream, ToSocketAddrs};

use postgres_protocol::message::{frontend, backend};
use postgres_shared::params::IntoConnectParams;
use bytes::{BufMut, BytesMut};

pub struct Connection {
    stream: BufWriter<TcpStream>,
    input: BytesMut,
    output: Vec<u8>,
}

impl Connection {
    pub fn connect<P: ToSocketAddrs>(addr: P) -> io::Result<Self> {
        // TODO(gandro): eventually we want to support IntoConnectParams
        // let params = params.into_connect_params().map_err(|e| {
        //     io::Error::new(io::ErrorKind::Other, e)
        // })?;
        Ok(Connection{
            stream: BufWriter::new(TcpStream::connect(addr)?),
            input: BytesMut::new(),
            output: Vec::new(),
        })
    }

    pub fn read(&mut self) -> io::Result<backend::Message> {
        const GROWTH_SIZE: usize = 64;
        // ensure we have some spare space
        if self.input.capacity() < GROWTH_SIZE {
            let additional = GROWTH_SIZE - self.input.capacity();
            self.input.reserve(additional)
        }

        loop {
            if let Some(msg) = backend::Message::parse(&mut self.input)? {
                return Ok(msg);
            }

            // read in as many bytes as we can get
            match self.stream.get_mut().read(unsafe { self.input.bytes_mut() })? {
                0 => return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof, "connection closed")),
                n => unsafe { self.input.advance_mut(n) },
            }

            // apparently the buffer is too small, extend it
            self.input.reserve(GROWTH_SIZE);
        }
    }
    
    pub fn write<'a>(&mut self, msg: &frontend::Message<'a>) -> io::Result<()> {
        self.output.clear();
        msg.serialize(&mut self.output)?;
        self.stream.write_all(&self.output)?;
        self.stream.flush()?;
        Ok(())
    }
}


#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
