package com.journaldev.spring;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.DatastoreException;
import com.google.cloud.datastore.DatastoreOptions;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.KeyFactory;
import com.google.cloud.datastore.Query;
import com.google.cloud.datastore.StringValue;
import com.google.cloud.datastore.StructuredQuery.OrderBy;
import com.google.cloud.datastore.Transaction;

/**
 * A simple Task List application demonstrating how to connect to Cloud Datastore, create, modify,
 * delete, and query entities.
 */
public class PersonGoogleCloudDatastore {

  // [START datastore_build_service]
  // Create an authorized Datastore service using Application Default Credentials.
  private final Datastore datastore = DatastoreOptions.getDefaultInstance().getService();

  // Create a Key factory to construct keys associated with this project.
  private final KeyFactory keyFactory = datastore.newKeyFactory().setKind("Person");

  public PersonGoogleCloudDatastore() {
	  
  }

  // [END datastore_build_service]

  // [START datastore_add_entity]
  /**
   * Adds a task entity to the Datastore.
   *
   * @param description The task description
   * @return The {@link Key} of the entity
   * @throws DatastoreException if the ID allocation or put fails
   */
  public String addPerson(String name) {
    Key key = datastore.allocateId(keyFactory.newKey());
    Entity task = Entity.newBuilder(key)
        .set("name", StringValue.newBuilder(name).setExcludeFromIndexes(true).build())
        .build();
    datastore.put(task);
    return "Person Added : " + name;
  }
  // [END datastore_add_entity]

  // [START datastore_update_entity]
  /**
   * Marks a task entity as done.
   *
   * @param id The ID of the task entity as given by {@link Key#id()}
   * @return true if the task was found, false if not
   * @throws DatastoreException if the transaction fails
   */
  boolean markDone(long id) {
    Transaction transaction = datastore.newTransaction();
    try {
      Entity task = transaction.get(keyFactory.newKey(id));
      if (task != null) {
        transaction.put(Entity.newBuilder(task).set("done", true).build());
      }
      transaction.commit();
      return task != null;
    } finally {
      if (transaction.isActive()) {
        transaction.rollback();
      }
    }
  }
  // [END datastore_update_entity]

  // [START datastore_retrieve_entities]
  /**
   * Returns a list of all task entities in ascending order of creation time.
   *
   * @throws DatastoreException if the query fails
   */
  public Iterator<Entity> listPersons() {
    Query<Entity> query =
        Query.newEntityQueryBuilder().setKind("Person").build();
    return datastore.run(query);
  }
  // [END datastore_retrieve_entities]

  // [START datastore_delete_entity]
  /**
   * Deletes a task entity.
   *
   * @param id The ID of the task entity as given by {@link Key#id()}
   * @throws DatastoreException if the delete fails
   */
  void deleteTask(long id) {
    datastore.delete(keyFactory.newKey(id));
  }
  // [END datastore_delete_entity]

  /**
   * Converts a list of task entities to a list of formatted task strings.
   *
   * @param tasks An iterator over task entities
   * @return A list of tasks strings, one per entity
   */
  public List<String> formatPersons(Iterator<Entity> persons) {
    List<String> strings = new ArrayList<>();
    while (persons.hasNext()) {
      Entity person = persons.next();
      strings.add(
              String.format("%d : %s ", person.getKey().getId(), person.getString("name")));
    }
    return strings;
  }

  /**
   * Handles a single command.
   *
   * @param commandLine A line of input provided by the user
   */
  void handleCommandLine(String commandLine) {
    String[] args = commandLine.split("\\s+");

    if (args.length < 1) {
      throw new IllegalArgumentException("not enough args");
    }

    String command = args[0];
    switch (command) {
      case "new":
        // Everything after the first whitespace token is interpreted to be the description.
        args = commandLine.split("\\s+", 2);
        if (args.length != 2) {
          throw new IllegalArgumentException("missing description");
        }
        // Set created to now() and done to false.
        addPerson(args[1]);
        System.out.println("task added");
        break;
      case "done":
        assertArgsLength(args, 2);
        long id = Long.parseLong(args[1]);
        if (markDone(id)) {
          System.out.println("task marked done");
        } else {
          System.out.printf("did not find a Task entity with ID %d%n", id);
        }
        break;
      case "list":
        assertArgsLength(args, 1);
        List<String> tasks = formatPersons(listPersons());
        System.out.printf("found %d tasks:%n", tasks.size());
        System.out.println("task ID : description");
        System.out.println("---------------------");
        for (String taskString : tasks) {
          System.out.println(taskString);
        }
        break;
      case "delete":
        assertArgsLength(args, 2);
        deleteTask(Long.parseLong(args[1]));
        System.out.println("task deleted (if it existed)");
        break;
      default:
        throw new IllegalArgumentException("unrecognized command: " + command);
    }
  }

  private void assertArgsLength(String[] args, int expectedLength) {
    if (args.length != expectedLength) {
      throw new IllegalArgumentException(
          String.format("expected exactly %d arg(s), found %d", expectedLength, args.length));
    }
  }

  /**
   * Exercises the methods defined in this class.
   *
   * <p>Assumes that you are authenticated using the Google Cloud SDK (using
   * {@code gcloud auth application-default login}).
   */
  public static void main(String[] args) throws Exception {
    PersonGoogleCloudDatastore taskList = new PersonGoogleCloudDatastore();
    System.out.println("Cloud Datastore Task List");
    System.out.println();
    printUsage();
    while (true) {
      String commandLine = System.console().readLine("> ");
      if (commandLine.trim().isEmpty()) {
        break;
      }
      try {
        taskList.handleCommandLine(commandLine);
      } catch (IllegalArgumentException e) {
        System.out.println(e.getMessage());
        printUsage();
      }
    }
    System.out.println("exiting");
    System.exit(0);
  }

  private static void printUsage() {
    System.out.println("Usage:");
    System.out.println();
    System.out.println("  new <description>  Adds a task with a description <description>");
    System.out.println("  done <task-id>     Marks a task as done");
    System.out.println("  list               Lists all tasks by creation time");
    System.out.println("  delete <task-id>   Deletes a task");
    System.out.println();
  }
}
