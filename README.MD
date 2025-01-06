

Proxy app to make queries 

## Getting started 

-  run:  uvicorn spanner_api:app --reload --log-level info



Considerations: 

- by default this project works with the folling Spanner config. 

SPANNER_PROJECT_ID = "test-project"
INSTANCE_ID = "test-instance"
DATABASE_ID = "test-database" 


### Query 

/execute-query 

POST method 

{
	"query" : <sql statement>
}


### Insert Update
/execute-mutation

POST metod  

POST method 

{
    operation: str
    table: str
    columns: List[str]
    values: List[ValueObject]
    keySet: Optional[Dict] = None
}