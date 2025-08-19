package consistent

import "fmt"

// GatewayMember gateway member implementation
type GatewayMember struct {
	ID   string // Gateway instance ID
	Host string // Host address
	Port int    // Port number
}

// Clone create deep copy of member, ensure concurrent safety
func (g *GatewayMember) Clone() *GatewayMember {
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
	return fmt.Sprintf("%s:%s:%d", g.ID, g.Host, g.Port)
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
	return fmt.Sprintf("%s:%d", g.Host, g.Port)
}
