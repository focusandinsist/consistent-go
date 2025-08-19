package consistent

import (
	"strconv"
	"strings"
)

// GatewayMember .
type GatewayMember struct {
	ID   string
	Host string
	Port int
}

// NewGatewayMember create new gateway member
func NewGatewayMember(id, host string, port int) *GatewayMember {
	return &GatewayMember{
		ID:   id,
		Host: host,
		Port: port,
	}
}

// Clone creates and returns a deep copy of the GatewayMember as a Member.
func (g *GatewayMember) Clone() Member {
	return &GatewayMember{
		ID:   g.ID,
		Host: g.Host,
		Port: g.Port,
	}
}

// String returns the string representation of GatewayMember in the form "ID:Host:Port".
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

// GetAddress returns the network address of the GatewayMember in the form "Host:Port".
func (g *GatewayMember) GetAddress() string {
	var builder strings.Builder
	portStr := strconv.Itoa(g.Port)
	builder.Grow(len(g.Host) + len(portStr) + 1) // +1 for colon
	builder.WriteString(g.Host)
	builder.WriteByte(':')
	builder.WriteString(portStr)
	return builder.String()
}
