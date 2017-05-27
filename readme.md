## JSQ - A mongoDB query parser for SQL databases

JSQ allows the use of similar mongoDB query expressions to find records in traditional SQL databases like Postgres, MySQL etc. 

### Installation
```
go get github.com/ncodes/jsq
```
### Example

```go

jsq, err := NewJSQ([]string{
    "valid_field_1",
    "name",
    "age",
})

// Parse a query
err := jsq.Parse(`{
    "name": { 
        "$eq": "ben" 
    }
}`)
if err != nil {
    log.Fatalf("failed to parse: %s", err)
}

// Get SQL 
sql, args, err := jsq.ToSQL()
```

#### Supported Compare Operators
- $eq  - Equal
- $gt  - Greater Than
- $gte - Greater Than or Equal
- $lt  - Less Than
- $lte - Less Than or Equal
- $ne  - Not Equal
- $in  - In array
- $nin - Not In array
- $not - Not (negate)
- $sw  - Starts with
- $ew  - End with
- $ct  - Contains

### Logical Operators
- $and - Find records matching every expression in an array 
- $or  - Find records matching at least an expression in an array
- $nor - Find records that fail to match all expressions in an array

### Links

- See full operator usage and examples on the [mongoDB website](https://docs.mongodb.com/manual/reference/operator/query/)
- [Go Doc](https://godoc.org/github.com/ncodes/jsq)

### Todo:
- More query operators
- Support joins and aggregate functions

