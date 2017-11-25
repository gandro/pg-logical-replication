extern crate postgres_logical_replication;
extern crate postgres_protocol;
extern crate fallible_iterator;

use std::fmt::{self, Debug};

use fallible_iterator::FallibleIterator;

use postgres_protocol::message::{frontend, backend};
use postgres_protocol::message::backend::ErrorFields;
use postgres_logical_replication::Connection;

fn fail<'a>(mut iter: ErrorFields<'a>) -> ! {
    while let Some(f) = iter.next().unwrap() {
        println!("{:?}={:?}", f.type_(), f.value());
    }
    panic!("Error!");
}

struct BackendDebug<'a>(&'a backend::Message);

impl<'a> Debug for BackendDebug<'a> {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        use postgres_protocol::message::backend::Message::*;
        match *self.0 {
            AuthenticationCleartextPassword => fmt.debug_tuple("AuthenticationCleartextPassword").finish(),
            AuthenticationGss => fmt.debug_tuple("AuthenticationGss").finish(),
            AuthenticationKerberosV5 => fmt.debug_tuple("AuthenticationKerberosV5").finish(),
            AuthenticationMd5Password(_) => fmt.debug_tuple("AuthenticationMd5Password").field(&"..").finish(),
            AuthenticationOk => fmt.debug_tuple("AuthenticationOk").finish(),
            AuthenticationScmCredential => fmt.debug_tuple("AuthenticationScmCredential").finish(),
            AuthenticationSspi => fmt.debug_tuple("AuthenticationSspi").finish(),
            AuthenticationGssContinue(_) => fmt.debug_tuple("AuthenticationGssContinue").field(&"..").finish(),
            AuthenticationSasl(_) => fmt.debug_tuple("AuthenticationSasl").field(&"..").finish(),
            AuthenticationSaslContinue(_) => fmt.debug_tuple("AuthenticationSaslContinue").field(&"..").finish(),
            AuthenticationSaslFinal(_) => fmt.debug_tuple("AuthenticationSaslFinal").field(&"..").finish(),
            BackendKeyData(_) => fmt.debug_tuple("BackendKeyData").field(&"..").finish(),
            BindComplete => fmt.debug_tuple("BindComplete").finish(),
            CloseComplete => fmt.debug_tuple("CloseComplete").finish(),
            CommandComplete(_) => fmt.debug_tuple("CommandComplete").field(&"..").finish(),
            CopyData(_) => fmt.debug_tuple("CopyData").field(&"..").finish(),
            CopyDone => fmt.debug_tuple("CopyDone").finish(),
            CopyInResponse(_) => fmt.debug_tuple("CopyInResponse").field(&"..").finish(),
            CopyOutResponse(_) => fmt.debug_tuple("CopyOutResponse").field(&"..").finish(),
            DataRow(_) => fmt.debug_tuple("DataRow").field(&"..").finish(),
            EmptyQueryResponse => fmt.debug_tuple("EmptyQueryResponse").finish(),
            ErrorResponse(ref e) => {
                let mut fmt = fmt.debug_struct("ErrorResponse");
                let mut iter = e.fields();
                while let Some(f) = iter.next().map_err(|_| fmt::Error)? {
                    fmt.field(&format!("{:?}", f.type_()), &format!("{:?}", f.value()));
                }
                fmt.finish()
            }
            NoData => fmt.debug_tuple("NoData").finish(),
            NoticeResponse(_) => fmt.debug_tuple("NoticeResponse").field(&"..").finish(),
            NotificationResponse(_) => fmt.debug_tuple("NotificationResponse").field(&"..").finish(),
            ParameterDescription(_) => fmt.debug_tuple("ParameterDescription").field(&"..").finish(),
            ParameterStatus(_) => fmt.debug_tuple("ParameterStatus").field(&"..").finish(),
            ParseComplete => fmt.debug_tuple("ParseComplete").finish(),
            PortalSuspended => fmt.debug_tuple("PortalSuspended").finish(),
            ReadyForQuery(_) => fmt.debug_tuple("ReadyForQuery").finish(),
            RowDescription(_) => fmt.debug_tuple("RowDescription").finish(),
            _ => fmt.debug_tuple("Unkown").finish(),
        }
    }
}

fn main() {
    let mut c = Connection::connect("localhost:5432").unwrap();
    c.write(&frontend::Message::StartupMessage {
        parameters: &[
            ("replication".into(), "true".into()),
            ("user".into(), "gandro".into()),
        ],
    }).unwrap();

    loop {
        let m = c.read().unwrap();
        println!("{:?}", BackendDebug(&m));
        match m {
            backend::Message::ParameterStatus(body) => {
                println!("Parameter {:?}={:?}", body.name(), body.value());
            },
            backend::Message::ReadyForQuery(body) => {
                println!("Status: {}", body.status());
                break;
            }
            _ => (),
        }
    }

    c.write(&frontend::Message::Query {
        query: "START_REPLICATION SLOT slot_name LOGICAL;"
    }).unwrap();
    let m = c.read().unwrap();
    println!("{:?}", BackendDebug(&m));


}
