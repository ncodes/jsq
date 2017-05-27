package jsq

import (
	"fmt"
	"strings"

	"reflect"

	"github.com/ellcrys/util"
	. "github.com/go-xorm/builder"
)

// Query defines an interface for JSQL query implementations
type Query interface {

	// Parse builds the query
	Parse(jsonQuery string) error

	// Set the table to query
	SetTable(table interface{}, plural bool)

	// ToSQL generates and returns the built SQL and arguments
	ToSQL() (string, []interface{}, error)
}

var (
	// ErrNotFound indicates a missing data
	ErrNotFound = fmt.Errorf("not found")

	logicalOperators = []string{
		"$and",
		"$or",
		"$nor",
	}

	compareOperators = []string{
		"$eq",  // equal
		"$gt",  // greater than
		"$gte", // greater than or equal
		"$lt",  // less than
		"$lte", // less than or equal
		"$ne",  // not equal
		"$in",  // in array
		"$nin", // not in array
		"$not", // not (negate)
		"$sw",  // starts with
		"$ew",  // end with
		"$ct",  // contains
	}
)

// parserCtx hold information about a JSQ to be parsed
type parserCtx struct {
	b      *Builder
	negate bool
}

// QueryOption provides fields that can be used to
// alter a query
type QueryOption struct {
	OrderBy string
	Limit   int
}

// JSQ defines a structure for constructing a query
// from json objects.
type JSQ struct {
	b *Builder

	// fieldWhitelist holds a list of valid field names
	fieldWhitelist []string
}

// NewJSQ connects to the database server and returns a new instance
func NewJSQ(fieldWhitelist []string) *JSQ {
	return &JSQ{
		fieldWhitelist: fieldWhitelist,
	}
}

// Parse prepares the JSQ instance to run the json JSQ by creating a new db scope
// containing all the JSQ requirements ready to be executed. It returns error
// if unable to parse jsonJSQ
func (q *JSQ) Parse(jsonJSQ string) error {
	var JSQ map[string]interface{}
	err := util.FromJSON([]byte(jsonJSQ), &JSQ)
	if err != nil {
		return fmt.Errorf("malformed json")
	}
	return q.parse(JSQ)
}

// isValidField checks whether a JSQ field is an acceptable field.
// If the valid fields whitelist is empty, all fields are considered valid
func (q JSQ) isValidField(f string) bool {
	return len(q.fieldWhitelist) == 0 || util.InStringSlice(q.fieldWhitelist, f)
}

// isValidOperator checks whether an operator is include
// in the operator set
func (q *JSQ) isValidOperator(op string, operatorSet []string) bool {
	return util.InStringSlice(operatorSet, op)
}

// getBuilder gets builder from a context if set
// or the builder in the JSQ
func (q *JSQ) getBuilder(ctx parserCtx) *Builder {
	if ctx.b == nil {
		return q.b
	}
	return ctx.b
}

// fieldExpr creates an express that will be prefixed with a NOT clause
// if negate is true.
func fieldExpr(negate bool, exp string, args ...interface{}) Cond {
	not := "NOT"
	if !negate {
		not = ""
	}
	return Expr(fmt.Sprintf("%s %s", not, exp), args...)
}

// parse parses the JSQ returning a slice of
// scope functions to pass to the new database scope.
func (q *JSQ) parse(JSQ map[string]interface{}) error {

	q.b = new(Builder)
	var _parse func(JSQStatement map[string]interface{}, ctx parserCtx) error

	// parses the JSQ
	_parse = func(JSQStatement map[string]interface{}, ctx parserCtx) error {

		for _field, _fieldValue := range JSQStatement {

			field := _field
			fieldValue := _fieldValue
			anOperator := field[0] == '$'

			// check if field is an operator and also a known top level operator
			if anOperator && !q.isValidOperator(field, logicalOperators) {
				return fmt.Errorf("unknown top level operator: %s", field)
			}

			// field is not an operator
			if !anOperator {

				// ensure the field name is valid
				if !q.isValidField(field) {
					return fmt.Errorf("unknown query field: %s", field)
				}

				// non-operator field can only have string, number of map value type
				if !q.isString(fieldValue) && !q.isNumber(fieldValue) && !q.isMap(fieldValue) {
					return fmt.Errorf("field '%s': invalid value type. expects string, number or map", field)
				}

				// when field value is a string, or number, add equality condition
				if q.isString(fieldValue) || q.isNumber(fieldValue) {
					q.getBuilder(ctx).And(fieldExpr(ctx.negate, fmt.Sprintf("%s = ?", field), fieldValue))
					continue
				}

				// at this point, the field value is a map
				// ensure all map keys are valid operators
				if err := q.validateCompareOperators(fieldValue); err != nil {
					return fmt.Errorf("field '%s': %s", field, err)
				}

				for _op, _opVal := range fieldValue.(map[string]interface{}) {
					op, opVal := _op, _opVal
					switch op {
					case "$eq":
						if !q.isString(opVal) && !q.isNumber(opVal) {
							return fmt.Errorf("field '%s': '$eq' operator supports only string and number type", field)
						}
						q.getBuilder(ctx).And(fieldExpr(ctx.negate, fmt.Sprintf("%s = ?", field), opVal))

					case "$gt":
						if !q.isString(opVal) && !q.isNumber(opVal) {
							return fmt.Errorf("field '%s': '$gt' operator supports only number or string type", field)
						}
						q.getBuilder(ctx).And(fieldExpr(ctx.negate, fmt.Sprintf("%s > ?", field), opVal))

					case "$gte":
						if !q.isString(opVal) && !q.isNumber(opVal) {
							return fmt.Errorf("field '%s': '$gte' operator supports only number or string type", field)
						}
						q.getBuilder(ctx).And(fieldExpr(ctx.negate, fmt.Sprintf("%s >= ?", field), opVal))

					case "$lt":
						if !q.isString(opVal) && !q.isNumber(opVal) {
							return fmt.Errorf("field '%s': '$lt' operator supports only number or string type", field)
						}
						q.getBuilder(ctx).And(fieldExpr(ctx.negate, fmt.Sprintf("%s < ?", field), opVal))

					case "$lte":
						if !q.isString(opVal) && !q.isNumber(opVal) {
							return fmt.Errorf("field '%s': '$lte' operator supports only number or string type", field)
						}
						q.getBuilder(ctx).And(fieldExpr(ctx.negate, fmt.Sprintf("%s <= ?", field), opVal))

					case "$ne":
						if !q.isString(opVal) && !q.isNumber(opVal) {
							return fmt.Errorf("field '%s': '$ne' operator supports only number or string type", field)
						}
						q.getBuilder(ctx).And(fieldExpr(ctx.negate, fmt.Sprintf("%s <> ?", field), opVal))

					case "$in":
						if !q.isArray(opVal) {
							return fmt.Errorf("field '%s': '$in' operator supports only array type", field)
						}
						values := opVal.([]interface{})
						placeHolders := strings.TrimRight(strings.Repeat("?,", len(values)), ",")
						q.getBuilder(ctx).And(fieldExpr(ctx.negate, fmt.Sprintf(`%s IN (`+placeHolders+`)`, field), values...))

					case "$nin":
						if !q.isArray(opVal) {
							return fmt.Errorf("field '%s': '$nin' operator supports only array type", field)
						}
						values := opVal.([]interface{})
						placeHolders := strings.TrimRight(strings.Repeat("?,", len(values)), ",")
						q.getBuilder(ctx).And(fieldExpr(ctx.negate, fmt.Sprintf(`%s NOT IN (`+placeHolders+`)`, field), values...))

					case "$sw":
						if !q.isString(opVal) {
							return fmt.Errorf("field '%s': '$sw' operator supports only string type", field)
						}
						value := opVal.(string)
						if strings.Index(value, "%") != -1 || strings.Index(value, "_") != -1 {
							return fmt.Errorf("field '%s': '$ew' string cannot contain these characters: %v", field, []string{"_", "%"})
						}
						q.getBuilder(ctx).And(fieldExpr(ctx.negate, fmt.Sprintf("%s LIKE ?", field), value+"%"))

					case "$ew":
						if !q.isString(opVal) {
							return fmt.Errorf("field '%s': '$ew' operator supports only string type", field)
						}
						value := opVal.(string)
						if strings.Index(value, "%") != -1 || strings.Index(value, "_") != -1 {
							return fmt.Errorf("field '%s': '$ew' string cannot contain these characters: %v", field, []string{"_", "%"})
						}
						q.getBuilder(ctx).And(fieldExpr(ctx.negate, fmt.Sprintf("%s LIKE ?", field), "%"+value))

					case "$ct":
						if !q.isString(opVal) {
							return fmt.Errorf("field '%s': '$ct' operator supports only string type", field)
						}
						value := opVal.(string)
						if strings.Index(value, "%") != -1 || strings.Index(value, "_") != -1 {
							return fmt.Errorf("field '%s': '$ct' string cannot contain these characters: %v", field, []string{"_", "%"})
						}
						q.getBuilder(ctx).And(fieldExpr(ctx.negate, fmt.Sprintf("%s LIKE ?", field), "%"+value+"%"))

					case "$not":
						if !q.isMap(opVal) {
							return fmt.Errorf("field '%s': '$not' operator supports only map type", field)
						}

						// ensure only compare operators are included
						for op := range _opVal.(map[string]interface{}) {
							if !q.isValidOperator(op, compareOperators) {
								return fmt.Errorf("bad value. unknown operator: %s", field)
							}
						}

						// construct new express using the field and the $not operator value
						// for direct comparison. eg: { field: { $not: { $eq: "xyz" }}} to { field: { $eq: "xyz" }}
						normalizedExpr := map[string]interface{}{
							field: opVal,
						}

						// parse the normalized expression and set negate to true in parser context
						err := _parse(normalizedExpr, parserCtx{
							b:      ctx.b,
							negate: true,
						})
						if err != nil {
							return err
						}
					}
				}
				continue
			}

			// At this point, field is an operator.
			// Field must be an array of expressions
			if !q.isArray(fieldValue) {
				return fmt.Errorf("field '%s': operator supports only array type", field)
			}

			switch field {
			case "$and":
				ctxBuilder := new(Builder)
				for _, stmt := range fieldValue.([]interface{}) {

					// statements must be maps
					if !q.isMap(stmt) {
						return fmt.Errorf("field '%s': '$and/$or' entries must be full objects", field)
					}

					// parse statement. Set a custom builder for the parsers
					err := _parse(stmt.(map[string]interface{}), parserCtx{b: ctxBuilder})
					if err != nil {
						return err
					}
				}

				// add conditions generated from the $and operation into the main builder
				ctxSQL, args, err := ctxBuilder.ToSQL()
				if err != nil {
					return fmt.Errorf("failed to construct sql from context builder")
				}

				// add sql to main or context builder
				q.getBuilder(ctx).And(Expr(ctxSQL, args...))

			case "$or":
				conditions := []Cond{}
				for _, stmt := range fieldValue.([]interface{}) {
					ctxBuilder := new(Builder)

					// statements must be maps
					if !q.isMap(stmt) {
						return fmt.Errorf("field '%s': '$and/$or' entries must be full objects", field)
					}

					// parse statement. Set a custom builder for the parsers
					err := _parse(stmt.(map[string]interface{}), parserCtx{b: ctxBuilder})
					if err != nil {
						return err
					}

					// create condition from context builder
					sql, args, err := ctxBuilder.ToSQL()
					if err != nil {
						return fmt.Errorf("failed to get sql from builder. %s", err)
					}

					conditions = append(conditions, Expr(sql, args...))
				}

				// add conditions to main or context builder
				q.getBuilder(ctx).And(Or(conditions...))

			case "$nor":
				conditions := []Cond{}
				for _, stmt := range fieldValue.([]interface{}) {
					ctxBuilder := new(Builder)

					// statements must be maps
					if !q.isMap(stmt) {
						return fmt.Errorf("field '%s': '$and/$or' entries must be full objects", field)
					}

					// parse statement. Set a custom builder for the parsers and set negate to true
					err := _parse(stmt.(map[string]interface{}), parserCtx{b: ctxBuilder, negate: true})
					if err != nil {
						return err
					}

					// create condition from context builder
					sql, args, err := ctxBuilder.ToSQL()
					if err != nil {
						return fmt.Errorf("failed to get sql from builder. %s", err)
					}

					conditions = append(conditions, Expr(sql, args...))
				}

				// add conditions to main or context builder
				q.getBuilder(ctx).And(And(conditions...))
			}
		}
		return nil
	}

	return _parse(JSQ, parserCtx{})
}

// isArray checks whether an interface underlying type is an array
func (q *JSQ) isArray(v interface{}) bool {
	if _, ok := v.([]interface{}); ok {
		return true
	}
	return false
}

// isString checks whether an interface underlying type is string
func (q *JSQ) isString(v interface{}) bool {
	if _, ok := v.(string); ok {
		return true
	}
	return false
}

// isNumber checks whether a value is a number
func (q *JSQ) isNumber(v interface{}) bool {
	switch v.(type) {
	case int, int32, int64, float32, float64:
		return true
	default:
		return false
	}
}

// isMap checks whether an interface value is a map[string]interface{}
func (q *JSQ) isMap(v interface{}) bool {
	if _, ok := v.(map[string]interface{}); ok {
		return true
	}
	return false
}

// validateCompareOperators takes a map of operators and
// checks if all operators are valid compare operators
func (q *JSQ) validateCompareOperators(v interface{}) error {
	if !q.isMap(v) {
		return fmt.Errorf("expected a map type")
	}
	for op := range v.(map[string]interface{}) {
		if !q.isValidOperator(op, compareOperators) {
			return fmt.Errorf("bad value. unknown operator: %s", op)
		}
	}
	return nil
}

// isEmptyBuilder checks whether the builder is empty.
// A builder with no condition will be empty
func (q *JSQ) isEmptyBuilder() bool {
	return q.b == nil || reflect.DeepEqual(q.b, new(Builder))
}

// getSQL gets SQL from the builder
func (q *JSQ) getSQL() (string, []interface{}, error) {
	var err error
	var stmt string
	var args []interface{}
	if !q.isEmptyBuilder() {
		stmt, args, err = q.b.ToSQL()
		if err != nil {
			return "", nil, err
		}
	}
	return stmt, args, err
}

// ToSQL returns the generated SQL and arguments
func (q *JSQ) ToSQL() (string, []interface{}, error) {
	if q.isEmptyBuilder() {
		return "", nil, nil
	}
	return q.b.ToSQL()
}
