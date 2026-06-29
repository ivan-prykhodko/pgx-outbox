package outbox

import "fmt"

// Route contains destination information for a message.
type Route interface {
	Data() map[string]any
}

// RouteResolver is a function that determines the route for a given message.
type RouteResolver func(msg *Message) (Route, error)

// Router resolves messages to their destination routes.
type Router interface {
	// Resolve returns a route for the given message.
	Resolve(msg *Message) (Route, error)
}

type router struct {
	resolvers map[string]RouteResolver
}

func NewRouter(resolvers map[string]RouteResolver) Router {
	return &router{
		resolvers: resolvers,
	}
}

func (r *router) Resolve(msg *Message) (Route, error) {
	routeName := RouteName(msg.AggregateType, msg.EventType)

	resolver, ok := r.resolvers[routeName]
	if !ok {
		return nil, fmt.Errorf("no route resolver for %s", routeName)
	}

	return resolver(msg)
}

func RouteName(aggregateType string, eventType string) string {
	return fmt.Sprintf("%s.%s", aggregateType, eventType)
}
