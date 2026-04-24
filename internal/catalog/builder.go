package catalog

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	"moe-asset-server/internal/config"
	"moe-asset-server/internal/cryptor"
	"moe-asset-server/internal/protocol"

	"github.com/bytedance/sonic"
	"github.com/dlclark/regexp2"
	"github.com/go-resty/resty/v2"
)

type Builder struct {
	cfg *config.Config
}

func NewBuilder(cfg *config.Config) *Builder {
	return &Builder{cfg: cfg}
}

func (b *Builder) BuildTasks(ctx context.Context, req protocol.JobRequest, downloaded map[string]string) (protocol.JobRequest, []protocol.TaskPayload, error) {
	regionCfg, ok := b.cfg.Regions[req.Server]
	if !ok {
		return req, nil, fmt.Errorf("region %s not found in config", req.Server)
	}
	if !regionCfg.Enabled {
		return req, nil, fmt.Errorf("region %s is disabled", req.Server)
	}

	client := newHTTPClient(regionCfg, b.cfg.Server.Proxy)
	cookie := ""
	if regionCfg.Provider.RequiredCookies {
		var err error
		cookie, err = b.parseCookies(ctx, req.Server, client)
		if err != nil {
			return req, nil, err
		}
	}

	var err error
	regionCfg, req.AssetVersion, req.AssetHash, err = b.resolveAssetVersion(ctx, client, req, regionCfg)
	if err != nil {
		return req, nil, err
	}

	info, err := b.fetchAssetBundleInfo(ctx, client, req.Server, regionCfg, req.AssetVersion, req.AssetHash)
	if err != nil {
		return req, nil, err
	}

	tasks := b.buildDownloadList(req.Server, regionCfg, info, downloaded, req.AssetVersion, req.AssetHash, cookie)
	return req, tasks, nil
}

func newHTTPClient(regionCfg config.RegionConfig, proxy string) *resty.Client {
	client := resty.New()
	client.
		SetRetryCount(0).
		SetTransport(&http.Transport{
			MaxIdleConnsPerHost: 100,
			IdleConnTimeout:     30 * time.Second,
			TLSHandshakeTimeout: 10 * time.Second,
			DisableKeepAlives:   false,
		}).
		SetHeader("Accept", "*/*").
		SetHeader("User-Agent", defaultGameUserAgent()).
		SetHeader("Connection", "keep-alive").
		SetHeader("Accept-Encoding", "gzip, deflate, br").
		SetHeader("Accept-Language", "zh-CN,zh-Hans;q=0.9")
	if regionCfg.Runtime.UnityVersion != "" {
		client.SetHeader("X-Unity-Version", regionCfg.Runtime.UnityVersion)
	}
	if proxy != "" {
		client.SetProxy(proxy)
	}
	return client
}

func defaultGameUserAgent() string {
	return "ProductName/134 CFNetwork/1408.0.4 Darwin/22.5.0"
}

func (b *Builder) parseCookies(ctx context.Context, region protocol.Region, client *resty.Client) (string, error) {
	if region != protocol.RegionJP {
		return "", nil
	}
	var lastErr error
	for attempt := 0; attempt < retryAttempts(b.cfg.Execution.Retry.Attempts); attempt++ {
		resp, err := client.R().SetContext(ctx).Post("https://issue.sekai.colorfulpalette.org/api/signature")
		if err != nil {
			lastErr = err
			time.Sleep(time.Second)
			continue
		}
		if resp.StatusCode() == http.StatusOK {
			cookie := resp.Header().Get("Set-Cookie")
			if cookie != "" {
				client.SetHeader("Cookie", cookie)
			}
			return cookie, nil
		}
		lastErr = fmt.Errorf("signature endpoint returned %d", resp.StatusCode())
		time.Sleep(time.Second)
	}
	if lastErr == nil {
		lastErr = errors.New("failed to fetch cookies")
	}
	return "", lastErr
}

func retryAttempts(configured int) int {
	if configured <= 0 {
		return 4
	}
	return configured
}

func (b *Builder) resolveAssetVersion(ctx context.Context, client *resty.Client, req protocol.JobRequest, regionCfg config.RegionConfig) (config.RegionConfig, string, string, error) {
	assetVersion := strings.TrimSpace(req.AssetVersion)
	assetHash := strings.TrimSpace(req.AssetHash)

	if regionCfg.Provider.CurrentVersionURL != "" {
		current, err := b.fetchCurrentVersion(ctx, client, regionCfg.Provider.CurrentVersionURL)
		if err != nil {
			return regionCfg, "", "", err
		}
		if current.AppVersion != "" {
			regionCfg.Provider.AppVersion = current.AppVersion
		}
		if current.SystemProfile != "" {
			regionCfg.Provider.Profile = current.SystemProfile
		}
		if isColorful(regionCfg.Provider.Kind) || regionCfg.Provider.AssetVersionURL == "" {
			if assetVersion == "" {
				assetVersion = current.AssetVersion
				if assetVersion == "" {
					assetVersion = current.DataVersion
				}
			}
			if assetHash == "" {
				assetHash = current.AssetHash
			}
		}
	}

	if isColorful(regionCfg.Provider.Kind) {
		if assetVersion == "" || assetHash == "" {
			return regionCfg, "", "", errors.New("failed to resolve assetVersion/assetHash from current_version_url; pass assetVersion and assetHash explicitly or configure current_version_url")
		}
		return regionCfg, assetVersion, assetHash, nil
	}

	if regionCfg.Provider.AppVersion == "" {
		return regionCfg, "", "", errors.New("failed to resolve appVersion from current_version_url; pass app_version in config or configure current_version_url")
	}
	if regionCfg.Provider.AssetVersionURL != "" {
		versionURL := strings.ReplaceAll(regionCfg.Provider.AssetVersionURL, "{app_version}", regionCfg.Provider.AppVersion)
		resp, err := requestWithRetry(ctx, client, versionURL)
		if err != nil {
			return regionCfg, "", "", err
		}
		if resp.StatusCode() != http.StatusOK {
			return regionCfg, "", "", fmt.Errorf("asset_version_url %s returned %d", versionURL, resp.StatusCode())
		}
		assetVersion = strings.TrimSpace(resp.String())
	}
	if assetVersion == "" {
		return regionCfg, "", "", errors.New("failed to resolve assetVersion from current_version_url or asset_version_url")
	}
	return regionCfg, assetVersion, assetHash, nil
}

type currentVersionInfo struct {
	SystemProfile string `json:"systemProfile"`
	AppVersion    string `json:"appVersion"`
	DataVersion   string `json:"dataVersion"`
	AssetVersion  string `json:"assetVersion"`
	AppHash       string `json:"appHash"`
	AssetHash     string `json:"assetHash"`
}

func (b *Builder) fetchCurrentVersion(ctx context.Context, client *resty.Client, url string) (currentVersionInfo, error) {
	resp, err := requestWithRetry(ctx, client, url)
	if err != nil {
		return currentVersionInfo{}, err
	}
	if resp.StatusCode() != http.StatusOK {
		return currentVersionInfo{}, fmt.Errorf("current_version_url returned %d", resp.StatusCode())
	}
	current := parseCurrentVersion(resp.Body())
	if current.AssetVersion == "" && current.DataVersion == "" && current.AppVersion == "" {
		return currentVersionInfo{}, errors.New("current_version_url did not contain appVersion, assetVersion or dataVersion")
	}
	return current, nil
}

func parseCurrentVersion(body []byte) currentVersionInfo {
	var current currentVersionInfo
	_ = sonic.Unmarshal(body, &current)
	if current.AssetVersion != "" || current.AssetHash != "" || current.AppVersion != "" || current.DataVersion != "" {
		return current
	}

	var payload any
	if err := sonic.Unmarshal(body, &payload); err != nil {
		return currentVersionInfo{}
	}
	current.SystemProfile = findStringByKey(payload, map[string]bool{"systemprofile": true, "system_profile": true})
	current.AppVersion = findStringByKey(payload, map[string]bool{"appversion": true, "app_version": true})
	current.DataVersion = findStringByKey(payload, map[string]bool{"dataversion": true, "data_version": true})
	current.AssetVersion = findStringByKey(payload, map[string]bool{
		"assetversion":       true,
		"asset_version":      true,
		"assetbundleversion": true,
	})
	current.AssetHash = findStringByKey(payload, map[string]bool{
		"assethash":       true,
		"asset_hash":      true,
		"assetbundlehash": true,
	})
	return current
}

func findStringByKey(v any, keys map[string]bool) string {
	switch x := v.(type) {
	case map[string]any:
		for key, value := range x {
			normalized := strings.ToLower(strings.ReplaceAll(key, "-", "_"))
			if keys[normalized] {
				if s, ok := value.(string); ok {
					return s
				}
			}
		}
		for _, value := range x {
			if found := findStringByKey(value, keys); found != "" {
				return found
			}
		}
	case []any:
		for _, value := range x {
			if found := findStringByKey(value, keys); found != "" {
				return found
			}
		}
	}
	return ""
}

func (b *Builder) fetchAssetBundleInfo(ctx context.Context, client *resty.Client, region protocol.Region, regionCfg config.RegionConfig, assetVersion string, assetHash string) (*assetBundleInfo, error) {
	assetURL, err := b.assetInfoURL(ctx, client, region, regionCfg, assetVersion, assetHash)
	if err != nil {
		return nil, err
	}
	resp, err := requestWithRetry(ctx, client, assetURL+timeArg())
	if err != nil {
		return nil, err
	}
	if resp.StatusCode() != http.StatusOK {
		return nil, fmt.Errorf("asset info %s returned %d", assetURL, resp.StatusCode())
	}
	c, err := cryptor.NewSekaiCryptorFromHex(regionCfg.Crypto.AESKeyHex, regionCfg.Crypto.AESIVHex)
	if err != nil {
		return nil, err
	}
	var info assetBundleInfo
	if err := c.UnpackInto(resp.Body(), &info); err != nil {
		return nil, err
	}
	if info.Bundles == nil {
		return nil, errors.New("asset info does not contain bundles")
	}
	return &info, nil
}

func (b *Builder) assetInfoURL(ctx context.Context, client *resty.Client, region protocol.Region, regionCfg config.RegionConfig, assetVersion string, assetHash string) (string, error) {
	provider := regionCfg.Provider
	if isColorful(provider.Kind) || region == protocol.RegionJP || region == protocol.RegionEN {
		profile := provider.Profile
		profileHash := ""
		if provider.ProfileHashes != nil {
			profileHash = provider.ProfileHashes[profile]
		}
		url := provider.AssetInfoURLTemplate
		url = strings.ReplaceAll(url, "{env}", profile)
		url = strings.ReplaceAll(url, "{hash}", profileHash)
		url = strings.ReplaceAll(url, "{asset_version}", assetVersion)
		url = strings.ReplaceAll(url, "{asset_hash}", assetHash)
		return url, nil
	}

	if assetVersion == "" {
		return "", errors.New("assetVersion is required for nuverse provider")
	}
	url := provider.AssetInfoURLTemplate
	url = strings.ReplaceAll(url, "{app_version}", provider.AppVersion)
	url = strings.ReplaceAll(url, "{asset_version}", assetVersion)
	return url, nil
}

func requestWithRetry(ctx context.Context, client *resty.Client, url string) (*resty.Response, error) {
	var lastErr error
	for attempt := 0; attempt < 4; attempt++ {
		resp, err := client.R().SetContext(ctx).Get(url)
		if err != nil {
			lastErr = err
			time.Sleep(time.Second)
			continue
		}
		if resp.StatusCode() >= http.StatusInternalServerError {
			lastErr = fmt.Errorf("server returned %d", resp.StatusCode())
			time.Sleep(time.Second)
			continue
		}
		return resp, nil
	}
	if lastErr == nil {
		lastErr = errors.New("request failed after retries")
	}
	return nil, lastErr
}

func (b *Builder) buildDownloadList(region protocol.Region, regionCfg config.RegionConfig, info *assetBundleInfo, downloaded map[string]string, assetVersion string, assetHash string, cookie string) []protocol.TaskPayload {
	tasks := make([]downloadTask, 0, len(info.Bundles))
	for bundleName, bundleInfo := range info.Bundles {
		if shouldSkipBundle(bundleName, regionCfg.Filters.Skip) {
			continue
		}
		if !shouldDownloadBundle(bundleName, bundleInfo.Category, regionCfg.Filters) {
			continue
		}
		bundleHash := bundleInfo.Hash
		if isNuverse(region, regionCfg.Provider.Kind) {
			bundleHash = fmt.Sprintf("%d", bundleInfo.Crc)
		}
		if existingHash, exists := downloaded[bundleName]; exists && existingHash == bundleHash {
			continue
		}
		downloadPath := getDownloadPath(region, regionCfg.Provider.Kind, bundleName, bundleInfo)
		tasks = append(tasks, downloadTask{
			downloadPath: downloadPath,
			bundlePath:   bundleName,
			bundleHash:   bundleHash,
			category:     bundleInfo.Category,
			priority:     getDownloadPriority(bundleName, regionCfg.Filters.Priority),
		})
	}
	sort.SliceStable(tasks, func(i, j int) bool {
		if tasks[i].priority == tasks[j].priority {
			return tasks[i].bundlePath < tasks[j].bundlePath
		}
		return tasks[i].priority < tasks[j].priority
	})

	payloads := make([]protocol.TaskPayload, 0, len(tasks))
	for _, task := range tasks {
		payloads = append(payloads, protocol.TaskPayload{
			Region:       region,
			BundlePath:   task.bundlePath,
			DownloadPath: task.downloadPath,
			BundleHash:   task.bundleHash,
			Category:     task.category,
			DownloadURL:  buildAssetBundleURL(region, regionCfg, task.downloadPath, assetVersion, assetHash) + timeArg(),
			Headers:      taskHeaders(regionCfg, cookie),
			Export:       regionCfg.ExportOptions(),
		})
	}
	return payloads
}

func taskHeaders(regionCfg config.RegionConfig, cookie string) map[string]string {
	headers := map[string]string{
		"Accept":          "*/*",
		"User-Agent":      defaultGameUserAgent(),
		"Connection":      "keep-alive",
		"Accept-Encoding": "gzip, deflate, br",
		"Accept-Language": "zh-CN,zh-Hans;q=0.9",
	}
	if regionCfg.Runtime.UnityVersion != "" {
		headers["X-Unity-Version"] = regionCfg.Runtime.UnityVersion
	}
	if cookie != "" {
		headers["Cookie"] = cookie
	}
	return headers
}

func buildAssetBundleURL(region protocol.Region, regionCfg config.RegionConfig, downloadPath string, assetVersion string, assetHash string) string {
	provider := regionCfg.Provider
	url := strings.ReplaceAll(provider.AssetBundleURLTemplate, "{bundle_path}", downloadPath)
	if isColorful(provider.Kind) || region == protocol.RegionJP || region == protocol.RegionEN {
		profile := provider.Profile
		profileHash := ""
		if provider.ProfileHashes != nil {
			profileHash = provider.ProfileHashes[profile]
		}
		url = strings.ReplaceAll(url, "{asset_version}", assetVersion)
		url = strings.ReplaceAll(url, "{asset_hash}", assetHash)
		url = strings.ReplaceAll(url, "{env}", profile)
		url = strings.ReplaceAll(url, "{hash}", profileHash)
	} else {
		url = strings.ReplaceAll(url, "{app_version}", provider.AppVersion)
	}
	return url
}

func shouldSkipBundle(bundleName string, patterns []string) bool {
	for _, pattern := range patterns {
		if regexpMatch(pattern, bundleName) {
			return true
		}
	}
	return false
}

func shouldDownloadBundle(bundleName string, category protocol.AssetCategory, filters config.FiltersConfig) bool {
	switch category {
	case protocol.AssetCategoryStartApp:
		return anyRegexpMatch(filters.StartApp, bundleName)
	case protocol.AssetCategoryOnDemand:
		return anyRegexpMatch(filters.OnDemand, bundleName)
	default:
		return false
	}
}

func anyRegexpMatch(patterns []string, value string) bool {
	if len(patterns) == 0 {
		return false
	}
	for _, pattern := range patterns {
		if regexpMatch(pattern, value) {
			return true
		}
	}
	return false
}

func regexpMatch(pattern string, value string) bool {
	re, err := regexp2.Compile(pattern, 0)
	if err != nil {
		return false
	}
	matched, err := re.MatchString(value)
	return err == nil && matched
}

func getDownloadPath(region protocol.Region, providerKind string, bundleName string, bundleInfo assetBundleDetail) string {
	if !isNuverse(region, providerKind) {
		return bundleName
	}
	if bundleInfo.DownloadPath != nil && *bundleInfo.DownloadPath != "" {
		return fmt.Sprintf("%s/%s", *bundleInfo.DownloadPath, bundleName)
	}
	return bundleName
}

func getDownloadPriority(bundleName string, patterns []string) int {
	for idx, pattern := range patterns {
		if regexpMatch(pattern, bundleName) {
			return idx
		}
	}
	return 9999999
}

func isColorful(kind string) bool {
	return kind == "" || strings.EqualFold(kind, "colorful_palette") || strings.EqualFold(kind, "colorful")
}

func isNuverse(region protocol.Region, kind string) bool {
	if strings.EqualFold(kind, "nuverse") {
		return true
	}
	return region == protocol.RegionTW || region == protocol.RegionKR || region == protocol.RegionCN
}

func timeArg() string {
	loc, _ := time.LoadLocation("Asia/Tokyo")
	return fmt.Sprintf("?t=%s", time.Now().In(loc).Format("20060102150405"))
}
