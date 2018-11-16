package no.ssb.lds.core.persistence.foundationdb;

import no.ssb.lds.api.persistence.PersistenceDeletePolicy;
import no.ssb.lds.api.persistence.buffered.BufferedPersistence;
import no.ssb.lds.api.persistence.buffered.Document;
import no.ssb.lds.api.persistence.buffered.DocumentKey;
import no.ssb.lds.api.persistence.buffered.DocumentLeafNode;
import no.ssb.lds.api.persistence.buffered.DocumentResult;
import org.json.JSONArray;
import org.json.JSONObject;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class FoundationDBInitializerTest {

    final String namespace = "lds-provider-fdb-testng-ns";
    BufferedPersistence persistence = null;
    FoundationDBPersistence streaming = null;

    @BeforeClass
    public void setup() {
        streaming = new FoundationDBInitializer().initialize(
                namespace,
                Map.of("foundationdb.directory.node-prefix.hex", "3A",
                        "foundationdb.directory.content-prefix.hex", "3B"),
                Set.of("Person", "Address", "FunkyLongAddress"));
        persistence = new BufferedPersistence(streaming);
    }

    @AfterClass
    public void teardown() {
        streaming.close();
    }

    @Test
    public void thatDeleteAllVersionsWorks() {
        ZonedDateTime jan1624 = ZonedDateTime.of(1624, 1, 1, 12, 0, 0, (int) TimeUnit.MILLISECONDS.toNanos(0), ZoneId.of("Etc/UTC"));
        ZonedDateTime jan1626 = ZonedDateTime.of(1626, 1, 1, 12, 0, 0, (int) TimeUnit.MILLISECONDS.toNanos(0), ZoneId.of("Etc/UTC"));
        ZonedDateTime jan1664 = ZonedDateTime.of(1664, 1, 1, 12, 0, 0, (int) TimeUnit.MILLISECONDS.toNanos(0), ZoneId.of("Etc/UTC"));
        Document input0 = toDocument(namespace, "Address", "newyork", createAddress("", "NY", "USA"), jan1624);
        persistence.createOrOverwrite(input0).join();
        Document input1 = toDocument(namespace, "Address", "newyork", createAddress("New Amsterdam", "NY", "USA"), jan1626);
        persistence.createOrOverwrite(input1).join();
        Document input2 = toDocument(namespace, "Address", "newyork", createAddress("New York", "NY", "USA"), jan1664);
        persistence.createOrOverwrite(input2).join();
        //List<Document> outputAfterCreate = persistence.readAllVersions(namespace, "Address", "newyork", 100).join().getMatches();

        //assertTrue(outputAfterCreate.size() > 0);

        persistence.deleteAllVersions(namespace, "Address", "newyork", PersistenceDeletePolicy.FAIL_IF_INCOMING_LINKS).join();

        Iterator<Document> iterator = persistence.readAllVersions(namespace, "Address", "newyork", 100).join();

        assertEquals(size(iterator), 0);
    }

    int size(Iterator<?> iterator) {
        int i = 0;
        while (iterator.hasNext()) {
            iterator.next();
            i++;
        }
        return i;
    }

    @Test
    public void thatBasicCreateThenReadWorks() {
        //persistence.deleteAllVersions(namespace, "Person", "john", PersistenceDeletePolicy.FAIL_IF_INCOMING_LINKS).join();

        ZonedDateTime oct18 = ZonedDateTime.of(2018, 10, 7, 19, 49, 26, (int) TimeUnit.MILLISECONDS.toNanos(307), ZoneId.of("Etc/UTC"));
        Document input = toDocument(namespace, "Person", "john", createPerson("John", "Smith"), oct18);
        persistence.createOrOverwrite(input).join();
        Document output = persistence.read(oct18, namespace, "Person", "john").join().next();
        assertNotNull(output);
        assertFalse(output == input);
        assertEquals(output, input);
    }

    @Test
    public void thatBasicTimeBasedVersioningWorks() {
        //persistence.deleteAllVersions(namespace, "Address", "newyork", PersistenceDeletePolicy.FAIL_IF_INCOMING_LINKS).join();

        ZonedDateTime jan1624 = ZonedDateTime.of(1624, 1, 1, 12, 0, 0, (int) TimeUnit.MILLISECONDS.toNanos(0), ZoneId.of("Etc/UTC"));
        ZonedDateTime jan1626 = ZonedDateTime.of(1626, 1, 1, 12, 0, 0, (int) TimeUnit.MILLISECONDS.toNanos(0), ZoneId.of("Etc/UTC"));
        ZonedDateTime jan1664 = ZonedDateTime.of(1664, 1, 1, 12, 0, 0, (int) TimeUnit.MILLISECONDS.toNanos(0), ZoneId.of("Etc/UTC"));
        Document input0 = toDocument(namespace, "Address", "newyork", createAddress("", "NY", "USA"), jan1624);
        persistence.createOrOverwrite(input0).join();
        Document input1 = toDocument(namespace, "Address", "newyork", createAddress("New Amsterdam", "NY", "USA"), jan1626);
        persistence.createOrOverwrite(input1).join();
        Document input2 = toDocument(namespace, "Address", "newyork", createAddress("New York", "NY", "USA"), jan1664);
        persistence.createOrOverwrite(input2).join();
        Iterator<Document> iterator = persistence.readAllVersions(namespace, "Address", "newyork", 100).join();
        assertEquals(iterator.next(), input2);
        assertEquals(iterator.next(), input1);
        assertEquals(iterator.next(), input0);
        assertFalse(iterator.hasNext());
    }

    @Test
    public void thatDeleteMarkerWorks() {
        persistence.deleteAllVersions(namespace, "Address", "newyork", PersistenceDeletePolicy.FAIL_IF_INCOMING_LINKS).join();

        ZonedDateTime jan1624 = ZonedDateTime.of(1624, 1, 1, 12, 0, 0, (int) TimeUnit.MILLISECONDS.toNanos(0), ZoneId.of("Etc/UTC"));
        ZonedDateTime jan1626 = ZonedDateTime.of(1626, 1, 1, 12, 0, 0, (int) TimeUnit.MILLISECONDS.toNanos(0), ZoneId.of("Etc/UTC"));
        ZonedDateTime feb1663 = ZonedDateTime.of(1663, 2, 1, 0, 0, 0, (int) TimeUnit.MILLISECONDS.toNanos(0), ZoneId.of("Etc/UTC"));
        ZonedDateTime jan1664 = ZonedDateTime.of(1664, 1, 1, 12, 0, 0, (int) TimeUnit.MILLISECONDS.toNanos(0), ZoneId.of("Etc/UTC"));

        persistence.createOrOverwrite(toDocument(namespace, "Address", "newyork", createAddress("", "NY", "USA"), jan1624)).join();
        persistence.createOrOverwrite(toDocument(namespace, "Address", "newyork", createAddress("New Amsterdam", "NY", "USA"), jan1626)).join();
        persistence.createOrOverwrite(toDocument(namespace, "Address", "newyork", createAddress("New York", "NY", "USA"), jan1664)).join();

        assertEquals(size(persistence.readAllVersions(namespace, "Address", "newyork", 100).join()), 3);

        persistence.markDeleted(feb1663, namespace, "Address", "newyork", PersistenceDeletePolicy.FAIL_IF_INCOMING_LINKS).join();

        assertEquals(size(persistence.readAllVersions(namespace, "Address", "newyork", 100).join()), 4);

        persistence.delete(feb1663, namespace, "Address", "newyork", PersistenceDeletePolicy.FAIL_IF_INCOMING_LINKS).join();

        assertEquals(size(persistence.readAllVersions(namespace, "Address", "newyork", 100).join()), 3);

        persistence.markDeleted(feb1663, namespace, "Address", "newyork", PersistenceDeletePolicy.FAIL_IF_INCOMING_LINKS).join();

        assertEquals(size(persistence.readAllVersions(namespace, "Address", "newyork", 100).join()), 4);
    }

    @Test
    public void thatReadVersionsInRangeWorks() {
        persistence.deleteAllVersions(namespace, "Person", "john", PersistenceDeletePolicy.FAIL_IF_INCOMING_LINKS).join();

        ZonedDateTime aug92 = ZonedDateTime.of(1992, 8, 1, 13, 43, 20, (int) TimeUnit.MILLISECONDS.toNanos(301), ZoneId.of("Etc/UTC"));
        ZonedDateTime feb10 = ZonedDateTime.of(2010, 2, 3, 15, 45, 22, (int) TimeUnit.MILLISECONDS.toNanos(303), ZoneId.of("Etc/UTC"));
        ZonedDateTime nov13 = ZonedDateTime.of(2013, 11, 5, 17, 47, 24, (int) TimeUnit.MILLISECONDS.toNanos(305), ZoneId.of("Etc/UTC"));
        ZonedDateTime sep18 = ZonedDateTime.of(2018, 9, 6, 18, 48, 25, (int) TimeUnit.MILLISECONDS.toNanos(306), ZoneId.of("Etc/UTC"));
        ZonedDateTime oct18 = ZonedDateTime.of(2018, 10, 7, 19, 49, 26, (int) TimeUnit.MILLISECONDS.toNanos(307), ZoneId.of("Etc/UTC"));
        persistence.createOrOverwrite(toDocument(namespace, "Person", "john", createPerson("John", "Smith"), aug92)).join();
        persistence.createOrOverwrite(toDocument(namespace, "Person", "john", createPerson("James", "Smith"), nov13)).join();
        persistence.createOrOverwrite(toDocument(namespace, "Person", "john", createPerson("John", "Smith"), oct18)).join();

        assertEquals(size(persistence.readVersions(feb10, sep18, namespace, "Person", "john", 100).join()), 2);
    }


    @Test
    public void thatFindAllWithPathAndValueWorks() {
        persistence.deleteAllVersions(namespace, "Person", "john", PersistenceDeletePolicy.FAIL_IF_INCOMING_LINKS).join();
        persistence.deleteAllVersions(namespace, "Person", "jane", PersistenceDeletePolicy.FAIL_IF_INCOMING_LINKS).join();

        ZonedDateTime aug92 = ZonedDateTime.of(1992, 8, 1, 13, 43, 20, (int) TimeUnit.MILLISECONDS.toNanos(301), ZoneId.of("Etc/UTC"));
        ZonedDateTime sep94 = ZonedDateTime.of(1994, 9, 1, 13, 43, 20, (int) TimeUnit.MILLISECONDS.toNanos(301), ZoneId.of("Etc/UTC"));
        ZonedDateTime feb10 = ZonedDateTime.of(2010, 2, 3, 15, 45, 22, (int) TimeUnit.MILLISECONDS.toNanos(303), ZoneId.of("Etc/UTC"));
        ZonedDateTime nov13 = ZonedDateTime.of(2013, 11, 5, 17, 47, 24, (int) TimeUnit.MILLISECONDS.toNanos(305), ZoneId.of("Etc/UTC"));
        ZonedDateTime sep18 = ZonedDateTime.of(2018, 9, 6, 18, 48, 25, (int) TimeUnit.MILLISECONDS.toNanos(306), ZoneId.of("Etc/UTC"));
        ZonedDateTime oct18 = ZonedDateTime.of(2018, 10, 7, 19, 49, 26, (int) TimeUnit.MILLISECONDS.toNanos(307), ZoneId.of("Etc/UTC"));
        persistence.createOrOverwrite(toDocument(namespace, "Person", "john", createPerson("John", "Smith"), aug92)).join();
        persistence.createOrOverwrite(toDocument(namespace, "Person", "jane", createPerson("Jane", "Doe"), sep94)).join();
        persistence.createOrOverwrite(toDocument(namespace, "Person", "jane", createPerson("Jane", "Smith"), feb10)).join();
        persistence.createOrOverwrite(toDocument(namespace, "Person", "john", createPerson("James", "Smith"), nov13)).join();
        persistence.createOrOverwrite(toDocument(namespace, "Person", "john", createPerson("John", "Smith"), oct18)).join();

        Iterator<DocumentResult> iterator = persistence.find(sep18, namespace, "Person", "lastname", "Smith", 100).join();

        Document person1 = iterator.next().document();
        Document person2 = iterator.next().document();
        assertFalse(iterator.hasNext());

        if (person1.contains("firstname", "Jane")) {
            assertTrue(person2.contains("firstname", "James"));
        } else {
            assertTrue(person1.contains("firstname", "James"));
            assertTrue(person2.contains("firstname", "Jane"));
        }
    }


    @Test
    public void thatFindAllWorks() {
        // TODO Consider support for deleting entire entity in one operation...?
        persistence.deleteAllVersions(namespace, "Person", "john", PersistenceDeletePolicy.FAIL_IF_INCOMING_LINKS).join();
        persistence.deleteAllVersions(namespace, "Person", "jane", PersistenceDeletePolicy.FAIL_IF_INCOMING_LINKS).join();

        ZonedDateTime aug92 = ZonedDateTime.of(1992, 8, 1, 13, 43, 20, (int) TimeUnit.MILLISECONDS.toNanos(301), ZoneId.of("Etc/UTC"));
        ZonedDateTime sep94 = ZonedDateTime.of(1994, 9, 1, 13, 43, 20, (int) TimeUnit.MILLISECONDS.toNanos(301), ZoneId.of("Etc/UTC"));
        ZonedDateTime feb10 = ZonedDateTime.of(2010, 2, 3, 15, 45, 22, (int) TimeUnit.MILLISECONDS.toNanos(303), ZoneId.of("Etc/UTC"));
        ZonedDateTime dec11 = ZonedDateTime.of(2011, 12, 4, 16, 46, 23, (int) TimeUnit.MILLISECONDS.toNanos(304), ZoneId.of("Etc/UTC"));
        ZonedDateTime nov13 = ZonedDateTime.of(2013, 11, 5, 17, 47, 24, (int) TimeUnit.MILLISECONDS.toNanos(305), ZoneId.of("Etc/UTC"));
        ZonedDateTime oct18 = ZonedDateTime.of(2018, 10, 7, 19, 49, 26, (int) TimeUnit.MILLISECONDS.toNanos(307), ZoneId.of("Etc/UTC"));
        persistence.createOrOverwrite(toDocument(namespace, "Person", "john", createPerson("John", "Smith"), aug92)).join();
        persistence.createOrOverwrite(toDocument(namespace, "Person", "jane", createPerson("Jane", "Doe"), sep94)).join();
        persistence.createOrOverwrite(toDocument(namespace, "Person", "jane", createPerson("Jane", "Smith"), feb10)).join();
        persistence.createOrOverwrite(toDocument(namespace, "Person", "john", createPerson("James", "Smith"), nov13)).join();
        persistence.createOrOverwrite(toDocument(namespace, "Person", "john", createPerson("John", "Smith"), oct18)).join();

        Iterator<DocumentResult> iterator = persistence.findAll(dec11, namespace, "Person", 100).join();

        Document person1 = iterator.next().document();
        Document person2 = iterator.next().document();
        assertFalse(iterator.hasNext());

        if (person1.contains("firstname", "Jane")) {
            assertTrue(person2.contains("firstname", "John"));
        } else {
            assertTrue(person1.contains("firstname", "John"));
            assertTrue(person2.contains("firstname", "Jane"));
        }
    }

    @Test
    public void thatBigValueWorks() {
        persistence.deleteAllVersions(namespace, "FunkyLongAddress", "newyork", PersistenceDeletePolicy.FAIL_IF_INCOMING_LINKS).join();

        ZonedDateTime oct18 = ZonedDateTime.of(2018, 10, 7, 19, 49, 26, (int) TimeUnit.MILLISECONDS.toNanos(307), ZoneId.of("Etc/UTC"));
        ZonedDateTime now = ZonedDateTime.now(ZoneId.of("Etc/UTC"));

        String bigString = "12345678901234567890";
        for (int i = 0; i < 12; i++) {
            bigString = bigString + "_" + bigString;
        }
        System.out.println("Creating funky long address");
        persistence.createOrOverwrite(toDocument(namespace, "FunkyLongAddress", "newyork", createAddress(bigString, "NY", "USA"), oct18)).join();

        System.out.println("Finding funky long address by city");
        int findSize = size(persistence.find(now, namespace, "FunkyLongAddress", "city", bigString, 100).join());
        if (findSize != 1) {
            throw new IllegalStateException("Test failed! " + findSize);
        }
        System.out.println("Finding funky long address by city (with non-matching value)");
        int findExpectNoMatchSize = size(persistence.find(now, namespace, "FunkyLongAddress", "city", bigString + "1", 100).join());
        if (findExpectNoMatchSize != 0) {
            throw new IllegalStateException("Test failed! " + findExpectNoMatchSize);
        }
        System.out.println("Deleting funky long address");
        persistence.delete(oct18, namespace, "FunkyLongAddress", "newyork", PersistenceDeletePolicy.FAIL_IF_INCOMING_LINKS).join();

    }

    static JSONObject createPerson(String firstname, String lastname) {
        JSONObject person = new JSONObject();
        person.put("firstname", firstname);
        person.put("lastname", lastname);
        return person;
    }

    static JSONObject createAddress(String city, String state, String country) {
        JSONObject address = new JSONObject();
        address.put("city", city);
        address.put("state", state);
        address.put("country", country);
        return address;
    }

    static Document toDocument(String namespace, String entity, String id, JSONObject json, ZonedDateTime timestamp) {
        DocumentKey key = new DocumentKey(namespace, entity, id, timestamp);
        Map<String, DocumentLeafNode> leafNodeByPath = new TreeMap<>();
        addFragments(key, "$.", json, leafNodeByPath);
        return new Document(key, leafNodeByPath, false);
    }

    private static void addFragments(DocumentKey key, String pathPrefix, JSONObject json, Map<String, DocumentLeafNode> leafNodeByPath) {
        for (Map.Entry<String, Object> entry : json.toMap().entrySet()) {
            String path = entry.getKey();
            Object untypedValue = entry.getValue();
            if (JSONObject.NULL.equals(untypedValue)) {
            } else if (untypedValue instanceof JSONObject) {
                addFragments(key, pathPrefix + "." + path, (JSONObject) untypedValue, leafNodeByPath);
            } else if (untypedValue instanceof JSONArray) {
            } else if (untypedValue instanceof String) {
                String value = (String) untypedValue;
                leafNodeByPath.put(path, new DocumentLeafNode(key, path, value, 8 * 1024));
            } else if (untypedValue instanceof Number) {
            } else if (untypedValue instanceof Boolean) {
            }
        }
    }
}
