package no.ssb.lds.core.persistence.foundationdb;

import no.ssb.lds.api.persistence.Document;
import no.ssb.lds.api.persistence.Fragment;
import no.ssb.lds.api.persistence.PersistenceDeletePolicy;
import no.ssb.lds.api.persistence.PersistenceResult;
import org.json.JSONArray;
import org.json.JSONObject;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class FoundationDBInitializerTest {

    final String namespace = "lds-provider-fdb-testng-ns";
    FoundationDBPersistence persistence = null;

    @BeforeClass
    public void setup() {
        persistence = new FoundationDBInitializer().initialize(
                namespace,
                Map.of("node-prefix.hex", "3A",
                        "content-prefix.hex", "3B"),
                Set.of("Person", "Address", "FunkyLongAddress"));
    }

    @AfterClass
    public void teardown() {
        persistence.close();
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
        List<Document> outputAfterCreate = persistence.readAllVersions(namespace, "Address", "newyork", 100).join().getMatches();

        assertTrue(outputAfterCreate.size() > 0);

        persistence.deleteAllVersions(namespace, "Address", "newyork", PersistenceDeletePolicy.FAIL_IF_INCOMING_LINKS).join();

        List<Document> outputAfterDeleteAll = persistence.readAllVersions(namespace, "Address", "newyork", 100).join().getMatches();

        assertEquals(outputAfterDeleteAll.size(), 0);
    }

    @Test
    public void thatBasicCreateThenReadWorks() {
        persistence.deleteAllVersions(namespace, "Person", "john", PersistenceDeletePolicy.FAIL_IF_INCOMING_LINKS).join();

        ZonedDateTime oct18 = ZonedDateTime.of(2018, 10, 7, 19, 49, 26, (int) TimeUnit.MILLISECONDS.toNanos(307), ZoneId.of("Etc/UTC"));
        Document input = toDocument(namespace, "Person", "john", createPerson("John", "Smith"), oct18);
        persistence.createOrOverwrite(input).join();
        PersistenceResult result = persistence.read(oct18, namespace, "Person", "john").join();
        assertEquals(result.getMatches().size(), 1);
        Document output = result.getMatches().get(0);
        assertFalse(output == input);
        assertEquals(output, input);
    }

    @Test
    public void thatBasicTimeBasedVersioningWorks() {
        persistence.deleteAllVersions(namespace, "Address", "newyork", PersistenceDeletePolicy.FAIL_IF_INCOMING_LINKS).join();

        ZonedDateTime jan1624 = ZonedDateTime.of(1624, 1, 1, 12, 0, 0, (int) TimeUnit.MILLISECONDS.toNanos(0), ZoneId.of("Etc/UTC"));
        ZonedDateTime jan1626 = ZonedDateTime.of(1626, 1, 1, 12, 0, 0, (int) TimeUnit.MILLISECONDS.toNanos(0), ZoneId.of("Etc/UTC"));
        ZonedDateTime jan1664 = ZonedDateTime.of(1664, 1, 1, 12, 0, 0, (int) TimeUnit.MILLISECONDS.toNanos(0), ZoneId.of("Etc/UTC"));
        Document input0 = toDocument(namespace, "Address", "newyork", createAddress("", "NY", "USA"), jan1624);
        persistence.createOrOverwrite(input0).join();
        Document input1 = toDocument(namespace, "Address", "newyork", createAddress("New Amsterdam", "NY", "USA"), jan1626);
        persistence.createOrOverwrite(input1).join();
        Document input2 = toDocument(namespace, "Address", "newyork", createAddress("New York", "NY", "USA"), jan1664);
        persistence.createOrOverwrite(input2).join();
        List<Document> output = persistence.readAllVersions(namespace, "Address", "newyork", 100).join().getMatches();
        assertEquals(output.get(0), input0);
        assertEquals(output.get(1), input1);
        assertEquals(output.get(2), input2);
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

        assertEquals(persistence.readAllVersions(namespace, "Address", "newyork", 100).join().getMatches().size(), 3);

        persistence.markDeleted(feb1663, namespace, "Address", "newyork", PersistenceDeletePolicy.FAIL_IF_INCOMING_LINKS).join();

        assertEquals(persistence.readAllVersions(namespace, "Address", "newyork", 100).join().getMatches().size(), 4);

        persistence.delete(feb1663, namespace, "Address", "newyork", PersistenceDeletePolicy.FAIL_IF_INCOMING_LINKS).join();

        assertEquals(persistence.readAllVersions(namespace, "Address", "newyork", 100).join().getMatches().size(), 3);

        persistence.markDeleted(feb1663, namespace, "Address", "newyork", PersistenceDeletePolicy.FAIL_IF_INCOMING_LINKS).join();

        assertEquals(persistence.readAllVersions(namespace, "Address", "newyork", 100).join().getMatches().size(), 4);
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

        List<Document> matches = persistence.readVersions(feb10, sep18, namespace, "Person", "john", 100).join().getMatches();
        assertEquals(matches.size(), 2);
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

        List<Document> matches = persistence.find(sep18, namespace, "Person", "lastname", "Smith", 100).join().getMatches();

        assertEquals(matches.size(), 2);

        Document person1 = matches.get(0);
        Document person2 = matches.get(1);

        if (person1.getFragments().contains(new Fragment("firstname", "Jane"))) {
            assertTrue(person2.getFragments().contains(new Fragment("firstname", "James")));
        } else {
            assertTrue(person1.getFragments().contains(new Fragment("firstname", "James")));
            assertTrue(person2.getFragments().contains(new Fragment("firstname", "Jane")));
        }
    }


    @Test
    public void thatFindAllWorks() {
        // TODO Support for deleting entire entity in one operation...
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

        List<Document> matches = persistence.findAll(dec11, namespace, "Person", 100).join().getMatches();

        assertEquals(matches.size(), 2);

        Document person1 = matches.get(0);
        Document person2 = matches.get(1);

        if (person1.getFragments().contains(new Fragment("firstname", "Jane"))) {
            assertTrue(person2.getFragments().contains(new Fragment("firstname", "John")));
        } else {
            assertTrue(person1.getFragments().contains(new Fragment("firstname", "John")));
            assertTrue(person2.getFragments().contains(new Fragment("firstname", "Jane")));
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
        List<Document> shouldMatch = persistence.find(now, namespace, "FunkyLongAddress", "city", bigString, 100).join().getMatches();
        if (shouldMatch.size() != 1) {
            throw new IllegalStateException("Test failed! " + shouldMatch.size());
        }
        System.out.println("Finding funky long address by city (with non-matching value)");
        List<Document> shouldNotMatch = persistence.find(now, namespace, "FunkyLongAddress", "city", bigString + "1", 100).join().getMatches();
        if (shouldNotMatch.size() != 0) {
            throw new IllegalStateException("Test failed! " + shouldNotMatch.size());
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
        NavigableSet<Fragment> fragments = new TreeSet<>();
        addFragments("$.", json, fragments);
        return new Document(namespace, entity, id, timestamp, fragments, false);
    }

    private static void addFragments(String pathPrefix, JSONObject json, NavigableSet<Fragment> fragments) {
        for (Map.Entry<String, Object> entry : json.toMap().entrySet()) {
            String key = entry.getKey();
            Object untypedValue = entry.getValue();
            if (JSONObject.NULL.equals(untypedValue)) {
            } else if (untypedValue instanceof JSONObject) {
                addFragments(pathPrefix + "." + key, (JSONObject) untypedValue, fragments);
            } else if (untypedValue instanceof JSONArray) {
            } else if (untypedValue instanceof String) {
                String value = (String) untypedValue;
                fragments.add(new Fragment(key, value));
            } else if (untypedValue instanceof Number) {
            } else if (untypedValue instanceof Boolean) {
            }
        }
    }
}
