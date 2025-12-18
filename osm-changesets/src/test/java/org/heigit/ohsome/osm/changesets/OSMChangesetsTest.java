package org.heigit.ohsome.osm.changesets;

import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;
class OSMChangesetsTest {

    @Test
    void parseChangesets() throws IOException {
        var bytes = """
                 <osm>
                     <changeset id="7270516" created_at="2011-02-12T23:00:22Z" closed_at="2011-02-12T23:00:25Z" open="false" user="blue_fire" uid="272207" min_lat="49.0547483" min_lon="9.257826" max_lat="49.0560854" max_lon="9.2617221" num_changes="25" comments_count="0">
                      <tag k="comment" v="Ilsfeld - Freibadkorrekturen (erste grobe Umgestaltung)"/>
                      <tag k="created_by" v="JOSM/1.5 (3701 de)"/>
                     </changeset>
                 </osm>""".getBytes();

        var changesets = OSMChangesets.readChangesets(bytes);
        assertEquals(1, changesets.size());
        assertEquals(7270516L, changesets.getFirst().id());
    }
  
}