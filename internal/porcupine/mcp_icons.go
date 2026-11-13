package porcupine

import (
	_ "embed"
	"encoding/base64"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

//go:embed assets/mcp-icon-light.png
var mcpIconLight []byte

//go:embed assets/mcp-icon-dark.png
var mcpIconDark []byte

func mcpIcons() []mcp.Icon {
	return []mcp.Icon{
		{Source: "data:image/png;base64," + base64.StdEncoding.EncodeToString(mcpIconLight), MIMEType: "image/png", Sizes: []string{"96x96"}, Theme: mcp.IconThemeLight},
		{Source: "data:image/png;base64," + base64.StdEncoding.EncodeToString(mcpIconDark), MIMEType: "image/png", Sizes: []string{"96x96"}, Theme: mcp.IconThemeDark},
	}
}
