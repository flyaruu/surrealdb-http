
use std::{error::Error, fmt::Display};

use base64::{Engine as _, engine::general_purpose};
use serde_json::Value;
use simplehttp::simplehttp::SimpleHttpClient;

pub struct SurrealDbClient {
    base_url: String,
    namespace: String,
    database: String,
    auth_token: String,
    client: Box<dyn SimpleHttpClient>,
}

#[derive(Debug)]
pub struct SurrealDbError(String, Option<Box<dyn Error>>);

impl SurrealDbError {
    fn new(message: &str, cause: Option<Box<dyn Error>>)->Self {
        SurrealDbError(message.to_owned(), cause)
    }
}

impl Display for SurrealDbError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0.as_str())
    }
}
impl Error for SurrealDbError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.1.as_deref()
    }

    fn description(&self) -> &str {
        "description() is deprecated; use Display"
    }

    fn cause(&self) -> Option<&dyn Error> {
        self.source()
    }
}

impl SurrealDbClient {
    pub fn new(username: &str, password: &str, base_url: &str, namespace: &str, database: &str, client: Box<dyn SimpleHttpClient>)->Self {
        // let token = base64::Engine::
        let mut auth_token = String::new();
        general_purpose::STANDARD.encode_string(format!("{}:{}",username,password), &mut auth_token);

        Self { auth_token, base_url: base_url.to_owned(), namespace: namespace.to_owned(), database: database.to_owned(), client}
    }

    pub fn get(&mut self, table: &str, key: &str)->Result<Vec<u8>, SurrealDbError> {
        let headers = [
            ("DB",self.database.as_str()),
            ("NS",self.namespace.as_str()),
            ("Accept","application/json"),
            ("Authorization",&format!("Basic {}",self.auth_token))
        ];
        let url = format!("{}/key/{}/{}",self.base_url,table,key);
        let result = self.client.get(&url, &headers[..])
            .map_err(|e| SurrealDbError::new(&format!("Error getting table: {} id: {}",table,key),Some(Box::new(e))))?;
        Ok(result)
    }

    pub fn insert(&mut self, table: &str, key: Option<&str>, value: &[u8])->Result<Vec<u8>, SurrealDbError> {
        let headers = [
            ("DB",self.database.as_str()),
            ("NS",self.namespace.as_str()),
            ("Accept","application/json"),
            ("Authorization",&format!("Basic {}",self.auth_token))
        ];
        let url = match key {
            Some(key) => format!("{}/key/{}/{}",self.base_url,table,key),
            None => format!("{}/key/{}",self.base_url,table),
        };
        let inserted = self.client.post(&url, &headers,value)
            .map_err(|e| SurrealDbError::new(&format!("Error querying table: {} key: {:?}",table,key),Some(Box::new(e))))?;
        Ok(inserted)
    }

    pub fn insert_for_id(&mut self, table: &str, value: &[u8])->Result<String, SurrealDbError> {
        let res = self.insert(table, None, value)?;
        let v: Value = serde_json::from_slice(&res).unwrap();
        let first_result = v.as_array().unwrap().first().unwrap().as_object().unwrap().get("result").unwrap().as_array().unwrap().first().unwrap();
        let id = first_result.get("id").unwrap().as_str().unwrap().to_owned();
        Ok(id)
    }

    pub fn is_healthy(&mut self)->bool {
        let headers = [
            ("Accept","application/json"),
        ];
        let url = format!("{}/health",self.base_url);
        self.client.get(&url, &headers).is_ok()
    }

    pub fn query(&mut self, query: &str)->Result<Vec<u8>, SurrealDbError> {
        let headers = [
            ("DB",self.database.as_str()),
            ("NS",self.namespace.as_str()),
            ("Accept","application/json"),
            ("Authorization",&format!("Basic {}",self.auth_token))
        ];
        let url = format!("{}/sql",self.base_url);
        let result = self.client.post(&url, &headers[..], query.as_bytes())
            .map_err(|e| SurrealDbError::new(&format!("Error querying: {}",query),Some(Box::new(e))))?;
        Ok(result)
    }
}

#[cfg(test)]
mod test {
    use std::{str::from_utf8};

    use serde_json::Value;
    use simplehttp::{simplehttp_reqwest::SimpleHttpClientReqwest};
    use super::SurrealDbClient;

    fn create_test_client()->SurrealDbClient {
        let client = SimpleHttpClientReqwest::new_reqwest().unwrap();
        SurrealDbClient::new("root", "root", "http://localhost:8000", "myns", "mydb", client)
    }

    #[test]
    fn test_health() {
        let mut surreal = create_test_client();
        assert!(surreal.is_healthy());
    }

    #[test]
    fn test_query() {
        let mut surreal = create_test_client();
        let res = surreal.query("select * from table").unwrap();
        let res = from_utf8(&res).unwrap();
        println!("Result: {}",res);
    }

    #[test]
    fn test_insert() {
        let mut surreal = create_test_client();
        let example = r#"{"kip":{"aap":"sji"},"aap":{"mies":"sjo"}}"#;
        for _ in 0..10 {
            let res = surreal.insert("test_table", None, example.as_bytes()).unwrap();
            println!("Result: {}",from_utf8(&res).unwrap());
        }
    }

    #[test]
    fn test_single(){
        let mut surreal = create_test_client();
        let res = surreal.get("test_table", "puz9ai2wrzcz52be7g04").unwrap();
        println!("Result: {}",from_utf8(&res).unwrap());
        let v: Value = serde_json::from_slice(&res).unwrap();
        let first_object = v.as_array().unwrap().first().unwrap().as_object().unwrap().get("result").unwrap().as_array().unwrap().first().unwrap();
        println!("Thing: {:?}",first_object);
        let id = first_object.get("id").unwrap().as_str().unwrap();
        assert_eq!("test_table:puz9ai2wrzcz52be7g04",id);
    }
}


