(function () {
    const MapImageUtils = {
        mapKey(name) {
            if (!name) return null;
            return String(name).trim().toLowerCase();
        },
        extractMapImage(entry) {
            if (!entry) return null;
            const curr = entry.curr || {};
            return (
                entry.image_sm || entry.imageSm ||
                entry.image_lg || entry.imageLg ||
                entry.image_sm_url || entry.image_url || entry.image ||
                curr.image_sm || curr.image_lg || curr.image || curr.logo ||
                entry.logo || entry.thumbnail || null
            );
        },
        buildMapImageLookup(stats, existing = {}) {
            const lookup = { ...(existing || {}) };
            if (!Array.isArray(stats)) return lookup;
            stats.forEach(item => {
                const key = this.mapKey(item?.map_name || item?.mapId || item?.pretty_name || item?.mapName || item?.map);
                const img = this.extractMapImage(item);
                if (key && img && !lookup[key]) {
                    lookup[key] = img;
                }
            });
            return lookup;
        },
        resolveMapImage(entry, options = {}) {
            if (!entry) return null;
            const direct = this.extractMapImage(entry);
            const key = this.mapKey(entry.map_name || entry.mapName || entry.map || entry.mapId || entry.pretty_name);
            const lookup = options.mapImageLookup || null;
            const catalog = options.mapCatalog || null;
            let resolved = direct;

            if (!resolved && key && lookup && lookup[key]) {
                resolved = lookup[key];
            }

            if (!resolved && key && Array.isArray(catalog)) {
                const match = catalog.find(item => {
                    const itemKey = this.mapKey(item?.map_id || item?.pretty_name || item?.map_name || item?.name || item?.mapName);
                    return itemKey && itemKey === key;
                });
                resolved = this.extractMapImage(match);
            }

            if (!resolved) return null;

            try {
                const apiClient = options.apiClient || window.apiClient;
                return apiClient && typeof apiClient.proxyAvatar === 'function'
                    ? apiClient.proxyAvatar(resolved)
                    : resolved;
            } catch (error) {
                return resolved;
            }
        },
        shouldFetchCatalog(stats) {
            if (!Array.isArray(stats) || !stats.length) return false;
            return stats.some(item => !this.extractMapImage(item) && this.mapKey(item?.map_name || item?.mapId || item?.pretty_name || item?.mapName || item?.map));
        }
    };

    window.MapImageUtils = MapImageUtils;
})();
