package outbox

import "fmt"

type Route struct {
	Topic          string
	Key            string
	IdempotencyKey string
}

type RouteResolver func(msg *Message) (Route, error)

type Router interface {
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
		return Route{}, fmt.Errorf("no route resolver for %s", routeName)
	}

	return resolver(msg)
}

func RouteName(aggregateType string, eventType string) string {
	return fmt.Sprintf("%s.%s", aggregateType, eventType)
}
