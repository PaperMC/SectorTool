package ca.spottedleaf.io.region;

import it.unimi.dsi.fastutil.ints.Int2ObjectLinkedOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMaps;
import java.util.Collections;

public final class MinecraftRegionFileType {

    private static final Int2ObjectLinkedOpenHashMap<MinecraftRegionFileType> BY_ID = new Int2ObjectLinkedOpenHashMap<>();
    private static final Int2ObjectLinkedOpenHashMap<String> NAME_TRANSLATION = new Int2ObjectLinkedOpenHashMap<>();
    private static final Int2ObjectMap<String> TRANSLATION_TABLE = Int2ObjectMaps.unmodifiable(NAME_TRANSLATION);

    public static final MinecraftRegionFileType CHUNK = new MinecraftRegionFileType("region", 0, "chunk_data");
    public static final MinecraftRegionFileType POI = new MinecraftRegionFileType("poi", 1, "poi_chunk");
    public static final MinecraftRegionFileType ENTITY = new MinecraftRegionFileType("entities", 2, "entity_chunk");

    private final String folder;
    private final int id;
    private final String name;

    public MinecraftRegionFileType(final String folder, final int id, final String name) {
        if (BY_ID.putIfAbsent(id, this) != null) {
            throw new IllegalStateException("Duplicate ids");
        }
        NAME_TRANSLATION.put(id, name);

        this.folder = folder;
        this.id = id;
        this.name = name;
    }

    public String getName() {
        return this.name;
    }

    public String getFolder() {
        return this.folder;
    }

    public int getNewId() {
        return this.id;
    }

    public static MinecraftRegionFileType byId(final int id) {
        return BY_ID.get(id);
    }

    public static String getName(final int id) {
        final MinecraftRegionFileType type = byId(id);
        return type == null ? null : type.getName();
    }

    public static Iterable<MinecraftRegionFileType> getAll() {
        return Collections.unmodifiableCollection(BY_ID.values());
    }

    public static Int2ObjectMap<String> getTranslationTable() {
        return TRANSLATION_TABLE;
    }
}
