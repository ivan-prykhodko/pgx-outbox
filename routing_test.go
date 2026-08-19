package outbox

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockRoute struct {
	data map[string]any
}

func (m mockRoute) Data() map[string]any {
	return m.data
}

func TestRouteName(t *testing.T) {
	assert.Equal(t, "order.created", RouteName("order", "created"))
	assert.Equal(t, "user.updated", RouteName("user", "updated"))
}

func TestRouter_Resolve(t *testing.T) {
	route1 := mockRoute{data: map[string]any{"queue": "orders"}}
	route2 := mockRoute{data: map[string]any{"queue": "users"}}

	resolvers := map[string]RouteResolver{
		"order.created": func(msg Message) (Route, error) {
			return route1, nil
		},
		"user.updated": func(msg Message) (Route, error) {
			return route2, nil
		},
	}

	r := NewRouter(resolvers)

	t.Run("resolves known route", func(t *testing.T) {
		msg := Message{AggregateType: "order", EventType: "created"}
		route, err := r.Resolve(msg)
		require.NoError(t, err)
		assert.Equal(t, route1, route)
	})

	t.Run("returns error for unknown route", func(t *testing.T) {
		msg := Message{AggregateType: "product", EventType: "deleted"}
		route, err := r.Resolve(msg)
		assert.Error(t, err)
		assert.Nil(t, route)
		assert.Contains(t, err.Error(), "no route resolver for product.deleted")
	})

	t.Run("returns error if resolver fails", func(t *testing.T) {
		expectedErr := assert.AnError
		r := NewRouter(map[string]RouteResolver{
			"order.failed": func(msg Message) (Route, error) {
				return nil, expectedErr
			},
		})

		msg := Message{AggregateType: "order", EventType: "failed"}
		route, err := r.Resolve(msg)
		assert.ErrorIs(t, err, expectedErr)
		assert.Nil(t, route)
	})
}
