package config

import "testing"

func TestRegionConfigExportOptionsIncludesMeshOBJ(t *testing.T) {
	region := RegionConfig{
		Export: ExportConfig{
			Mesh: MeshExportConfig{
				ExportOBJ: true,
				PathPatterns: []string{
					`^mysekai/fixture(?:/|$)`,
					`^mysekai/site/field/my_room_asset/skin(?:/|$)`,
				},
			},
		},
	}

	options := region.ExportOptions()
	if !options.ExportMeshOBJ {
		t.Fatalf("expected ExportMeshOBJ to be true")
	}
	if len(options.MeshOBJPathPatterns) != 2 {
		t.Fatalf("unexpected mesh path pattern count: got %d", len(options.MeshOBJPathPatterns))
	}
	if options.MeshOBJPathPatterns[0] != `^mysekai/fixture(?:/|$)` {
		t.Fatalf("unexpected first mesh path pattern: %q", options.MeshOBJPathPatterns[0])
	}

	region.Export.Mesh.PathPatterns[0] = `^changed/`
	if options.MeshOBJPathPatterns[0] != `^mysekai/fixture(?:/|$)` {
		t.Fatalf("expected ExportOptions to copy mesh path patterns, got %q", options.MeshOBJPathPatterns[0])
	}
}

func TestRegionConfigExportOptionsMeshOBJDefaultsDisabled(t *testing.T) {
	options := (RegionConfig{}).ExportOptions()
	if options.ExportMeshOBJ {
		t.Fatalf("expected ExportMeshOBJ to default to false")
	}
	if len(options.MeshOBJPathPatterns) != 0 {
		t.Fatalf("expected no default mesh path patterns, got %v", options.MeshOBJPathPatterns)
	}
}
