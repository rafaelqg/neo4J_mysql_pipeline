//docker run --name neo4j_container -p 7475:7474 -p 7688:7687 -d -v /neo4j/data:/data -v /neo4j/logs:/logs -v /neo4j/import:/var/lib/neo4j/import -v /neo4j/plugins:/plugins --env NEO4J_AUTH=neo4j/password neo4j:latest
const neo4j = require('neo4j-driver');// npm install --save neo4j-driver​
const mysql = require('mysql');

//initialize mysql connection
const MYSQL_IP="localhost";
const MYSQL_LOGIN="root";
const MYSQL_PASSWORD="root";

let con = mysql.createConnection({
  host:  MYSQL_IP,
  user: MYSQL_LOGIN,
  password: MYSQL_PASSWORD,
  database: "sakila"
});

const driver = neo4j.driver('bolt://localhost:7688',neo4j.auth.basic('neo4j', "password"), {});

const clearNeo4jNodesAndEdges = "MATCH (n) DETACH DELETE n";

async function execCypher(cypher){
  const neo4JSession = driver.session({database:"neo4j"});
  let cypherResult = await neo4JSession.run(cypher);
  neo4JSession.close();
  return cypherResult;
}

async function processData(){
  let cypherResult = await execCypher(clearNeo4jNodesAndEdges);
  //console.log("cypherResult", cypherResult);
  const select_actors = "SELECT actor_id, first_name, last_name from actor";
  con.query(select_actors, function (err, result) {
    result.forEach(async record=>{
      //console.log("Actors:",record);
      let cypherCreateActorNode = "CREATE (n:Actor { first_name: '"+record.first_name+"', last_name: '"+ record.last_name.replace("'","") +"', actor_id: "+record.actor_id+"})";
      cypherResult = await execCypher(cypherCreateActorNode);
      //console.log("cypherResult", cypherResult);
    });
    const select_films = "SELECT film_id, title from film";
    con.query(select_films, async function (err, result) {
      result.forEach(async record=>{
        //console.log("Films:",record);
        let cypherCreateFilmNode = "CREATE (n:Film { title: '"+record.title+"', film_id: "+record.film_id+"})";
        cypherResult = await execCypher(cypherCreateFilmNode);
        const film_id = record.film_id;
        const sql_actors_per_film = "SELECT actor_id FROM film_actor where film_id=?";
        con.query(sql_actors_per_film,[film_id], async function (err, result) {
          result.forEach(async record=>{
            let createEdgeFilmActor = `MATCH (a:Actor),(f:Film) WHERE a.actor_id = ${record.actor_id} AND f.film_id = ${film_id}
            CREATE (a)-[r:film_actor]->(f)`;
            cypherResult = await execCypher(createEdgeFilmActor);
            //console.log("actors from film:",film_id,record);
            //console.log(cypherResult);
          });
          
          //performing queries non Neo4J (test case)
          
          if(film_id === 2){
            let queryActorsPerFilmCypher = `MATCH (a:Actor)-[:film_actor]-(f:Film) where f.film_id=${film_id} return a,f`
            cypherResult = await execCypher(queryActorsPerFilmCypher);
           // console.log(cypherResult, JSON.stringify(cypherResult));
            console.log("=== records neo4J results===");
            cypherResult.records.forEach(el =>{
              //console.log("el", el);
              el._fields.forEach(f=>{
                console.log("neo4J node", "Properties:", f.properties, "labels", f.labels);
              });
            });
          }

          let queryActorsPerFilmSQL  = `SELECT a.actor_id, a.first_name, a.last_name, f.film_id, f.title FROM film_actor fa
          inner join sakila.actor as a  on fa.actor_id = a.actor_id 
          inner join film as f on  f.film_id = fa.film_id where f.film_id=${film_id}`;

        });
      });
    });
  });
}
processData();