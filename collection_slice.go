// Generated by: gen
// TypeWriter: slice
// Directive: +gen on Collection

package main

// CollectionSlice is a slice of type Collection. Use it where you would use []Collection.
type CollectionSlice []Collection

// Where returns a new CollectionSlice whose elements return true for func. See: http://clipperhouse.github.io/gen/#Where
func (rcv CollectionSlice) Where(fn func(Collection) bool) (result CollectionSlice) {
	for _, v := range rcv {
		if fn(v) {
			result = append(result, v)
		}
	}
	return result
}
