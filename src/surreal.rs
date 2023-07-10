
use std::{error::Error, fmt::Display, str::from_utf8};

use base64::{Engine as _, engine::general_purpose};
use serde::{Deserialize};
use serde_json::{Value, from_value};
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

#[derive(Deserialize,Debug,PartialEq, Eq)]
pub enum SurrealStatus {
    OK,
    ERROR,
}
#[derive(Deserialize,Debug)]
pub struct DynamicSurrealResult (Vec<DynamicSurrealStatementReply>);

impl DynamicSurrealResult {
    pub fn take_first(mut self)->Result<DynamicSurrealStatementReply,SurrealDbError> {
        self.0.pop().ok_or(SurrealDbError("Missing result field".to_owned(), None))
    }    
}
#[derive(Deserialize,Debug)]
pub struct DynamicSurrealStatementReply {
    status: SurrealStatus,
    result: Option<Vec<Value>>,
}

impl DynamicSurrealStatementReply {
    pub fn take_first(self)->Result<Value,SurrealDbError> {
        if self.status != SurrealStatus::OK {
            return Err(SurrealDbError(format!("Error reported in reply:{:?}",self.status), None))
        }
        let ll = self.result
            .ok_or(SurrealDbError("Missing result field".to_owned(), None))?
            .pop();
            
        ll.ok_or(SurrealDbError("Empty result field".to_owned(),None))
    }
}

#[derive(Deserialize,Debug)]
pub struct SurrealResult<T> (Vec<SurrealStatementReply<T>>);
#[derive(Deserialize,Debug)]
pub struct SurrealStatementReply<T> {
    status: SurrealStatus,
    result: Vec<T>,
}


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
    // Add builder pattern?
    pub fn new(username: &str, password: &str, base_url: &str, namespace: &str, database: &str, client: Box<dyn SimpleHttpClient>)->Self {
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

    pub fn delete(&mut self, table: &str, key: Option<&str>)->Result<Vec<u8>, SurrealDbError> {
        let headers = [
            ("DB",self.database.as_str()),
            ("NS",self.namespace.as_str()),
            ("Accept","application/json"),
            ("Authorization",&format!("Basic {}",self.auth_token))
        ];

        let url = match key {
            Some(key)=>format!("{}/key/{}/{}",self.base_url,table,key),
            None => format!("{}/key/{}",self.base_url,table),
        };
    
        let result = self.client.delete(&url, &headers[..])
            .map_err(|e| SurrealDbError::new(&format!("Error getting table: {} id: {:?}",table,key),Some(Box::new(e))))?;
        Ok(result)
    }

    // DynamicSurrealResult
    pub fn insert(&mut self, table: &str, key: Option<&str>, value: &[u8])->Result<DynamicSurrealResult, SurrealDbError> {
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
        let parsed: Value = serde_json::from_slice(&inserted)
            .map_err(|e| SurrealDbError::new(&format!("Error parsing json result from insert at table: {} key: {:?}",table, key),Some(Box::new(e))))?;

        let l = from_value::<DynamicSurrealResult>(parsed)
            .map_err(|e| SurrealDbError::new(&format!("Error interpreting json result from insert at table: {} key: {:?}",table, key),Some(Box::new(e))))?;

        Ok(l)
    }

    pub fn insert_for_id(&mut self, table: &str, value: &[u8])->Result<String, SurrealDbError> {
        let res = self.insert(table, None, value)?
            .0
            .pop()
            .ok_or(SurrealDbError("Missing result element".to_owned(),None))?
            .take_first()?;

        res.get("id")
            .ok_or(SurrealDbError("Missing id".to_owned(),None))?
            .as_str()
            .ok_or(SurrealDbError("Bad id".to_owned(),None))
            .map(|e|e.to_owned())
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

    pub fn query_dynamic_single(&mut self, query: &str)->Result<DynamicSurrealResult,SurrealDbError> {
        let value = self.query(query)?;
        let v: Value = serde_json::from_slice(&value)
            .map_err(|e| SurrealDbError::new(&format!("Error parsing json result: {}",query),Some(Box::new(e))))?;
        // let l = v.as_array().unwrap().first().unwrap();
        // println!("Thing: {:?}",l);
        let l = from_value::<DynamicSurrealResult>(v)
            .map_err(|e| SurrealDbError::new(&format!("Error parsing json result: {}",query),Some(Box::new(e))))?;
        Ok(l)
    }
    pub fn query_single<T>(&mut self, query: &str)->Result<SurrealStatementReply<T>,SurrealDbError> where T: for<'a> Deserialize<'a> {
        let value = self.query(query)?;
        let value_string = from_utf8(&value).unwrap();
        println!("{}",value_string);
        let mut result: SurrealResult<T> = serde_json::from_slice(&value)
            .map_err(|e| SurrealDbError::new(&format!("Error parsing json result: {}",query),Some(Box::new(e))))?;
        let first_result = result.0.pop().ok_or(SurrealDbError::new("Missing reply",None))?;

        Ok(first_result)
    }
    // pub fn query_single<'de, T> where T: Deserialize<'de> (&mut self, query: &str)->Result<SurrealResult<T>,SurrealDbError> where T: Deserialize {
    //     let value = self.query(query)?;
    //     let result: SurrealResult<T> = serde_json::from_slice(&value)
    //         .map_err(|e| SurrealDbError::new(&format!("Error parsing json result: {}",query),Some(Box::new(e))))?;
    //     Ok(result)
    // }
}

#[cfg(test)]
mod test {
    use std::{str::from_utf8};

    use serde::Deserialize;
    use serde_json::Value;
    use simplehttp::{simplehttp_reqwest::SimpleHttpClientReqwest};
    use crate::surreal::SurrealStatementReply;

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
        let res = surreal.query("select * from person").unwrap();
        let res = from_utf8(&res).unwrap();
        println!("Result: {}",res);
    }

    #[test]
    fn test_insert() {
        let mut surreal = create_test_client();
        // surreal.del
        let example = r#"{"kip":{"aap":"sji"},"aap":{"mies":"sjo"}}"#.as_bytes();
        for _ in 0..10 {
            let id = surreal.insert_for_id("test_table", example).unwrap();
            println!("Result: {}",id);
        }
    }

    #[test]
    fn test_single(){
        let mut surreal = create_test_client();
        let example = r#"{"kip":{"aap":"sji"},"aap":{"mies":"sjo"}}"#;
        surreal.insert("test_table", Some("puz9ai2wrzcz52be7g04"), example.as_bytes()).unwrap();
        let res = surreal.get("test_table", "puz9ai2wrzcz52be7g04").unwrap();
        println!("Result: {}",from_utf8(&res).unwrap());
        let v: Value = serde_json::from_slice(&res).unwrap();
        let first_object = v.as_array().unwrap().first().unwrap().as_object().unwrap().get("result").unwrap().as_array().unwrap().first().unwrap();
        println!("Thing: {:?}",first_object);
        let id = first_object.get("id").unwrap().as_str().unwrap();
        assert_eq!("test_table:puz9ai2wrzcz52be7g04",id);
    }

#[derive(Deserialize,Debug)]
    struct City {
        name: String
    }
#[test]
    fn test_query_single(){
 
        let mut surreal = create_test_client();
        surreal.delete("unit_query_single", None).unwrap();
        let city1 = r#"{"name":"Hanoi"}"#.as_bytes();
        let city2 = r#"{"name":"Isesaki"}"#.as_bytes();
        let city3 = r#"{"name":"Zeleznogorsk"}"#.as_bytes();
        let r1 = surreal.insert_for_id("unit_query_single", city1).unwrap();
        println!("Result: {}",r1);
        let r2 = surreal.insert_for_id("unit_query_single", city2).unwrap();
        println!("Result: {}",r2);
        let r3 = surreal.insert_for_id("unit_query_single", city3).unwrap();
        println!("Result: {}",r3);
        let res: SurrealStatementReply<City> = surreal.query_single("select name from unit_query_single;").expect("huh?");
        println!("RESULT: {:?}", res);
        let v = res.result.iter().map(|c|c.name.as_str()).collect::<Vec<&str>>();
        println!("RESULT2: {:?}", v);
        assert!(v.contains(&"Zeleznogorsk"));
        let res: SurrealStatementReply<City> = surreal.query_single("select name from unit_query_single;").expect("huh?");
        println!("RESULT2: {:?}", res);
        assert_eq!(3,res.result.len());
        let res = surreal.delete("unit_query_single", None).unwrap();
        println!("Result: {}",from_utf8(&res).unwrap());
        
        let res: SurrealStatementReply<City> = surreal.query_single("select name from unit_query_single;").expect("huh?");
        assert_eq!(0,res.result.len())  

    }

}


