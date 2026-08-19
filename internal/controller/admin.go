package controller

import (
	"embed"
	"html/template"
	"net/http"
)

//go:embed admin.html
var adminFS embed.FS

var adminTemplate *template.Template

func init() {
	var err error
	adminTemplate, err = template.ParseFS(adminFS, "admin.html")
	if err != nil {
		panic("failed to parse admin template: " + err.Error())
	}
}

// AdminHandler serves the admin panel UI.
type AdminHandler struct {
	state *ClusterState
}

// NewAdminHandler creates a new admin panel handler.
func NewAdminHandler(state *ClusterState) *AdminHandler {
	return &AdminHandler{state: state}
}

// ServeHTTP serves the admin panel HTML page.
func (h *AdminHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")

	data := struct {
		Title string
	}{
		Title: "KV-DB Admin Panel",
	}

	if err := adminTemplate.Execute(w, data); err != nil {
		http.Error(w, "Failed to render template", http.StatusInternalServerError)
		return
	}
}
