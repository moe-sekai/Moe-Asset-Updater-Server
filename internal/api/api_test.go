package api

import (
	"strings"
	"testing"

	"moe-asset-server/internal/protocol"
)

func TestValidateExpectedOutputsAudio(t *testing.T) {
	baseExport := protocol.ExportOptions{
		ExportACBFiles:    true,
		DecodeACBFiles:    true,
		DecodeHCAFiles:    true,
		ConvertAudioToMP3: true,
	}

	tests := []struct {
		name        string
		export      protocol.ExportOptions
		files       []string
		wantErr     string
		wantErrPart string
	}{
		{
			name:        "requires mp3 when wav remains without mp3",
			export:      baseExport,
			files:       []string{"sound/voice.wav"},
			wantErr:     "export requires mp3 conversion",
			wantErrPart: "wav=1",
		},
		{
			name: "requires wav removal when wav remains with mp3",
			export: protocol.ExportOptions{
				ExportACBFiles:    true,
				DecodeACBFiles:    true,
				DecodeHCAFiles:    true,
				ConvertAudioToMP3: true,
				RemoveWav:         true,
			},
			files:       []string{"sound/voice.mp3", "sound/voice.wav"},
			wantErr:     "export requires wav removal",
			wantErrPart: "wav_samples=[sound/voice.wav]",
		},
		{
			name: "mp3 only passes",
			export: protocol.ExportOptions{
				ExportACBFiles:    true,
				DecodeACBFiles:    true,
				DecodeHCAFiles:    true,
				ConvertAudioToMP3: true,
				RemoveWav:         true,
			},
			files: []string{"sound/voice.mp3"},
		},
		{
			name:   "no audio passes",
			export: baseExport,
			files:  []string{"image/card.webp"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateExpectedOutputs(manifestWithFiles(tt.files...), tt.export)
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("expected no error, got %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("expected error containing %q", tt.wantErr)
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("expected error containing %q, got %v", tt.wantErr, err)
			}
			if tt.wantErrPart != "" && !strings.Contains(err.Error(), tt.wantErrPart) {
				t.Fatalf("expected error containing %q, got %v", tt.wantErrPart, err)
			}
		})
	}
}

func manifestWithFiles(paths ...string) protocol.TaskResultManifest {
	files := make([]protocol.ResultFile, 0, len(paths))
	for _, path := range paths {
		files = append(files, protocol.ResultFile{Path: path})
	}
	return protocol.TaskResultManifest{Files: files}
}
