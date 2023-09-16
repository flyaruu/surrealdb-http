
use std::{error::Error, str::from_utf8};

use base64::{Engine as _, engine::general_purpose};
use serde::Deserialize;
use serde_json::{Value, from_value};
use simplehttp::simplehttp::{SimpleHttpClient, SimpleHttpError};
use thiserror::Error;

pub struct SurrealDbClient {
    base_url: String,
    namespace: String,
    database: String,
    auth_token: String,
    client: Box<dyn SimpleHttpClient>,
}

#[derive(Debug,Error)]
pub enum SurrealDbError {
    #[error("No result found")]
    NoResult,
    #[error("Not-ok status")]
    NotOkStatus(SurrealStatus),
    #[error("Empty result")]
    EmptyResult,
    #[error("Server error")]
    ServerError(String, SimpleHttpError),
    #[error("Other")]
    Other(String,Box<dyn Error>),
}

#[derive(Deserialize,Debug,PartialEq, Eq)]
pub enum SurrealStatus {
    OK,
    ERR,
}
#[derive(Deserialize,Debug)]
pub struct DynamicSurrealResult (Vec<DynamicSurrealStatementReply>);

impl DynamicSurrealResult {
    pub fn take_first(mut self)->Result<DynamicSurrealStatementReply,SurrealDbError> {
        self.0.pop().ok_or(SurrealDbError::NoResult)
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
            return Err(SurrealDbError::NotOkStatus(self.status))
        }
        let ll = self.result
            .ok_or(SurrealDbError::NoResult)?
            .pop();
            
        ll.ok_or(SurrealDbError::EmptyResult)
    }
}

#[derive(Deserialize,Debug)]
pub struct SurrealResult<T> (Vec<SurrealStatementReply<T>>);
#[derive(Deserialize,Debug)]
pub struct SurrealStatementReply<T> {
    pub status: SurrealStatus,
    pub result: Vec<T>,
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
            .map_err(|e| SurrealDbError::ServerError(format!("Error getting table: {} id: {}",table,key),e))?;
        Ok(result)
    }

    /// Delete the supplied key from the table. **If no key is supplied, the whole table is deleted**
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
            .map_err(|e| SurrealDbError::ServerError(format!("Error getting table: {} id: {:?}",table,key),e))?;
        Ok(result)
    }

    // DynamicSurrealResult
    fn insert(&mut self, table: &str, key: Option<&str>, value: &[u8])->Result<DynamicSurrealResult, SurrealDbError> {
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
            .map_err(|e| SurrealDbError::ServerError(format!("Error querying table: {} key: {:?}",table,key),e))?;
        let parsed: Value = serde_json::from_slice(&inserted)
            .map_err(|e| SurrealDbError::Other(format!("Error parsing json result from insert at table: {} key: {:?}",table, key),Box::new(e)))?;
        let l = from_value::<DynamicSurrealResult>(parsed)
            .map_err(|e| SurrealDbError::Other(format!("Error interpreting json result from insert at table: {} key: {:?}",table, key),Box::new(e)))?;
        Ok(l)
    }

    /// Insert a record, without an id, and return the generated id as string
    pub fn insert_for_id(&mut self, table: &str, value: &[u8])->Result<String, SurrealDbError> {
        let res = self.insert(table, None, value)?
            .0
            .pop()
            .ok_or(SurrealDbError::NoResult)?
            .take_first()?;

        res.get("id")
            .ok_or(SurrealDbError::NoResult)?
            .as_str()
            .ok_or(SurrealDbError::EmptyResult)
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
            .map_err(|e| SurrealDbError::ServerError(format!("Error querying: {}",query),e))?;
        Ok(result)
    }

    pub fn query_dynamic_single(&mut self, query: &str)->Result<DynamicSurrealResult,SurrealDbError> {
        let value = self.query(query)?;
        let v: Value = serde_json::from_slice(&value)
            .map_err(|e| SurrealDbError::Other(format!("Error parsing json result: {}",query),Box::new(e)))?;
        let l = from_value::<DynamicSurrealResult>(v)
            .map_err(|e| SurrealDbError::Other(format!("Error parsing json result: {}",query),Box::new(e)))?;
        Ok(l)
    }
    pub fn query_single<T>(&mut self, query: &str)->Result<SurrealStatementReply<T>,SurrealDbError> where T: for<'a> Deserialize<'a> {
        let value = self.query(query)?;
        let value_string = from_utf8(&value).unwrap();
        println!("{}",value_string);
        let mut result: SurrealResult<T> = serde_json::from_slice(&value)
            .map_err(|e| SurrealDbError::Other(format!("Error parsing json result: {}",query),Box::new(e)))?;
        let first_result = result.0.pop().ok_or(SurrealDbError::EmptyResult)?;
        Ok(first_result)
    }
}

#[cfg(test)]
mod test {
    use std::{str::from_utf8, env};

    use serde::Deserialize;
    use serde_json::Value;
    use simplehttp::simplehttp_reqwest::SimpleHttpClientReqwest;
    use crate::surreal::SurrealStatementReply;

    use super::SurrealDbClient;

    fn create_test_client()->SurrealDbClient {
        let host = env::var("SURREAL_URL").unwrap_or("http://localhost:8000".to_owned());
        // let host = env::var("SURREAL_URL").unwrap_or("http://10.11.12.213:8000".to_owned());
        let username = env::var("SURREAL_USER").unwrap_or("root".to_owned());
        let password = env::var("SURREAL_PASS").unwrap_or("root".to_owned());
        let namespace = env::var("SURREAL_NAMESPACE").unwrap_or("myns".to_owned());
        let database = env::var("SURREAL_DATABASE").unwrap_or("mydb".to_owned());

        let client = SimpleHttpClientReqwest::new_reqwest().unwrap();
        SurrealDbClient::new(&username, &password, &host, &namespace, &database, client)
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
        let v: Value = serde_json::from_slice(&res).unwrap();
        let first_object = v.as_array().unwrap().first().unwrap().as_object().unwrap().get("result").unwrap().as_array().unwrap().first().unwrap();
        let id = first_object.get("id").unwrap().as_str().unwrap();
        assert_eq!("test_table:puz9ai2wrzcz52be7g04",id);
        surreal.delete("test_table", Some("puz9ai2wrzcz52be7g04")).unwrap();
        // let res = surreal.get("test_table", "puz9ai2wrzcz52be7g04").unwrap();
        // println!("Result: {:?}",res);
        // assert!(surreal.get("test_table", "puz9ai2wrzcz52be7g04").is_err());
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
        let _ = surreal.insert_for_id("unit_query_single", city1).unwrap();
        let _ = surreal.insert_for_id("unit_query_single", city2).unwrap();
        let _ = surreal.insert_for_id("unit_query_single", city3).unwrap();
        let res: SurrealStatementReply<City> = surreal.query_single("select name from unit_query_single;").expect("huh?");
        let v = res.result.iter().map(|c|c.name.as_str()).collect::<Vec<&str>>();
        assert!(v.contains(&"Zeleznogorsk"));
        assert_eq!(3,res.result.len());
        let _ = surreal.delete("unit_query_single", None).unwrap();
        let res: SurrealStatementReply<City> = surreal.query_single("select name from unit_query_single;").expect("huh?");
        assert_eq!(0,res.result.len())  
    }

//     #[test]
//     fn test_complex_dynamic_query() {
//         let mut surreal = create_test_client();
//         let result = surreal.query_dynamic_single("SELECT *,->played_in->film.title as films FROM actor WHERE id=actor:1").unwrap();
//         println!("Result: {:?}",result);

//     }

//     #[test]
//     fn test_complex_static_query() {
//         #[derive(Deserialize,Debug)]
//         struct ActorWithFilms {
//             films: Vec<String>,
//             first_name: String,
//             last_name: String,
//             actor_id: usize,
//         }
//         let mut surreal = create_test_client();
//         let result = surreal.query_single::<ActorWithFilms>("SELECT *,->played_in->film.title as films FROM actor WHERE id=actor:1").unwrap();
//         println!("Result: {:?}",result.result.first().unwrap());
//     }

}


