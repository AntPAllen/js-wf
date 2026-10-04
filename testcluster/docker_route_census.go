//go:build linux

package testcluster

import (
	"context"
	"encoding/json"
	"fmt"
)

// RouteCensus counts distinct peers separately from pooled route connections.
// It describes one monitoring response, not quorum or stream catch-up.
type RouteCensus struct {
	ServerName  string            `json:"server_name"`
	ServerID    string            `json:"server_id"`
	Connections int               `json:"connections"`
	Peers       map[string]int    `json:"peers"`
	PeerIDs     map[string]string `json:"peer_ids"`
	Missing     []string          `json:"missing_peers"`
	FullMesh    bool              `json:"full_mesh"`
}

func (c *DockerCluster) RoutePeerCensus(ctx context.Context, node int) (RouteCensus, error) {
	data, err := c.Diagnostic(ctx, node, "routes")
	if err != nil {
		return RouteCensus{}, err
	}
	return decodeRouteCensus(data, c.names[node], c.names)
}
func decodeRouteCensus(data []byte, expected string, members []string) (RouteCensus, error) {
	var response struct {
		ServerName string `json:"server_name"`
		ServerID   string `json:"server_id"`
		NumRoutes  int    `json:"num_routes"`
		Routes     []struct {
			Name string `json:"remote_name"`
			ID   string `json:"remote_id"`
		} `json:"routes"`
	}
	if err := json.Unmarshal(data, &response); err != nil {
		return RouteCensus{}, err
	}
	if response.ServerName != expected || response.ServerID == "" || response.NumRoutes != len(response.Routes) {
		return RouteCensus{}, fmt.Errorf("invalid route census identity/count for %s", expected)
	}
	allowed := map[string]bool{}
	for _, name := range members {
		if name != expected {
			allowed[name] = true
		}
	}
	out := RouteCensus{ServerName: response.ServerName, ServerID: response.ServerID, Connections: response.NumRoutes, Peers: map[string]int{}, PeerIDs: map[string]string{}, Missing: []string{}}
	ids, names := map[string]string{}, map[string]string{}
	for _, route := range response.Routes {
		if !allowed[route.Name] || route.ID == "" || route.ID == response.ServerID || ids[route.Name] != "" && ids[route.Name] != route.ID || names[route.ID] != "" && names[route.ID] != route.Name {
			return RouteCensus{}, fmt.Errorf("invalid route peer identity %s/%s", route.Name, route.ID)
		}
		ids[route.Name], names[route.ID] = route.ID, route.Name
		out.PeerIDs[route.Name] = route.ID
		out.Peers[route.Name]++
	}
	for _, name := range members {
		if name != expected && out.Peers[name] == 0 {
			out.Missing = append(out.Missing, name)
		}
	}
	out.FullMesh = len(out.Missing) == 0
	return out, nil
}
