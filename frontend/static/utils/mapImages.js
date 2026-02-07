(function () {
    const MapImageUtils = {
        mapKey(name) {
            if (!name) return null;
            return String(name).trim().toLowerCase();
        },
        mapKeys(...values) {
            const keys = new Set();
            values.forEach(value => {
                const key = this.mapKey(value);
                if (!key) return;
                keys.add(key);
                if (key.startsWith('de_')) {
                    const shortKey = key.slice(3);
                    if (shortKey) keys.add(shortKey);
                } else {
                    keys.add(`de_${key}`);
                }
            });
            return Array.from(keys);
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
                const img = this.extractMapImage(item);
                if (!img) return;
                const keys = this.mapKeys(
                    item?.map_id,
                    item?.mapId,
                    item?.pretty_name,
                    item?.prettyName,
                    item?.map_name,
                    item?.mapName,
                    item?.name,
                    item?.map
                );
                keys.forEach(key => {
                    if (!lookup[key]) lookup[key] = img;
                });
            });
            return lookup;
        },
        resolveMapImage(entry, options = {}) {
            if (!entry) return null;
            const direct = this.extractMapImage(entry);
            const keys = this.mapKeys(
                entry.map_id,
                entry.mapId,
                entry.pretty_name,
                entry.prettyName,
                entry.map_name,
                entry.mapName,
                entry.name,
                entry.map
            );
            const lookup = options.mapImageLookup || null;
            const catalog = options.mapCatalog || null;
            let resolved = direct;

            if (!resolved && lookup && keys.length) {
                const lookupHit = keys.find(key => lookup[key]);
                if (lookupHit) {
                    resolved = lookup[lookupHit];
                }
            }

            if (!resolved && keys.length && Array.isArray(catalog)) {
                const match = catalog.find(item => {
                    const itemKeys = this.mapKeys(
                        item?.map_id,
                        item?.mapId,
                        item?.pretty_name,
                        item?.prettyName,
                        item?.map_name,
                        item?.mapName,
                        item?.name,
                        item?.map
                    );
                    return itemKeys.some(key => keys.includes(key));
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
            return stats.some(item => {
                if (this.extractMapImage(item)) return false;
                return this.mapKeys(
                    item?.map_id,
                    item?.mapId,
                    item?.pretty_name,
                    item?.prettyName,
                    item?.map_name,
                    item?.mapName,
                    item?.name,
                    item?.map
                ).length > 0;
            });
        }
    };

    window.MapImageUtils = MapImageUtils;
})();
