package consistent

import (
	"strconv"
	"strings"
)

// GatewayMember gateway member implementation
type GatewayMember struct {
	ID   string // Gateway instance ID
	Host string // Host address
	Port int    // Port number
}

// Clone create deep copy of member, ensure concurrent safety
func (g *GatewayMember) Clone() Member {
	return &GatewayMember{
		ID:   g.ID,
		Host: g.Host,
		Port: g.Port,
	}
}

// NewGatewayMember create new gateway member
func NewGatewayMember(id, host string, port int) *GatewayMember {
	return &GatewayMember{
		ID:   id,
		Host: host,
		Port: port,
	}
}

// String return string representation of member
func (g *GatewayMember) String() string {
	var builder strings.Builder
	portStr := strconv.Itoa(g.Port)
	builder.Grow(len(g.ID) + len(g.Host) + len(portStr) + 2) // +2 for colons
	builder.WriteString(g.ID)
	builder.WriteByte(':')
	builder.WriteString(g.Host)
	builder.WriteByte(':')
	builder.WriteString(portStr)
	return builder.String()
}

// GetID get gateway ID
func (g *GatewayMember) GetID() string {
	return g.ID
}

// GetHost get host address
func (g *GatewayMember) GetHost() string {
	return g.Host
}

// GetPort get port number
func (g *GatewayMember) GetPort() int {
	return g.Port
}

// GetAddress get complete address
func (g *GatewayMember) GetAddress() string {
	var builder strings.Builder
	portStr := strconv.Itoa(g.Port)
	builder.Grow(len(g.Host) + len(portStr) + 1) // +1 for colon
	builder.WriteString(g.Host)
	builder.WriteByte(':')
	builder.WriteString(portStr)
	return builder.String()
}
