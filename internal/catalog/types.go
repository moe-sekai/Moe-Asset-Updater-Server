package catalog

import "moe-asset-server/internal/protocol"

type assetTargetOS string

const (
	assetTargetOSiOS     assetTargetOS = "ios"
	assetTargetOSAndroid assetTargetOS = "android"
)

type assetBundleDetail struct {
	BundleName         string                 `msgpack:"bundleName"`
	CacheFileName      string                 `msgpack:"cacheFileName"`
	CacheDirectoryName string                 `msgpack:"cacheDirectoryName"`
	Hash               string                 `msgpack:"hash"`
	Category           protocol.AssetCategory `msgpack:"category"`
	Crc                int64                  `msgpack:"crc"`
	FileSize           int64                  `msgpack:"fileSize"`
	Dependencies       []string               `msgpack:"dependencies"`
	Paths              []string               `msgpack:"paths,omitempty"`
	IsBuiltin          bool                   `msgpack:"isBuiltin"`
	IsRelocate         *bool                  `msgpack:"isRelocate,omitempty"`
	Md5Hash            *string                `msgpack:"md5Hash,omitempty"`
	DownloadPath       *string                `msgpack:"downloadPath,omitempty"`
}

type assetBundleInfo struct {
	Version *string                      `msgpack:"version,omitempty"`
	OS      *assetTargetOS               `msgpack:"os,omitempty"`
	Bundles map[string]assetBundleDetail `msgpack:"bundles"`
}

type downloadTask struct {
	downloadPath       string
	bundlePath         string
	bundleHash         string
	category           protocol.AssetCategory
	priority           int
	estimatedSizeBytes int64
	delayed            bool
}
