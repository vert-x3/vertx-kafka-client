package io.vertx.kafka.client.common;

import io.vertx.codegen.annotations.DataObject;
import io.vertx.core.json.JsonObject;

/**
 * @author <a href="mailto:ppatierno@live.com">Paolo Patierno</a>
 */
@DataObject
public class Node {

  private boolean hasRack;
  private String host;
  private int id;
  private String idString;
  private boolean isEmpty;
  private int port;
  private String rack;

  /**
   * Constructor
   */
  public Node() {

  }

  /**
   * Constructor (from JSON representation)
   *
   * @param json  JSON representation
   */
  public Node(JsonObject json) {
    this.hasRack = json.getBoolean("hasRack");
    this.host = json.getString("host");
    this.id = json.getInteger("id");
    this.idString = json.getString("idString");
    this.isEmpty = json.getBoolean("isEmpty");
    this.port = json.getInteger("port");
    this.rack = json.getString("rack");
  }

  /**
   * True if this node has a defined rack
   *
   * @return
   */
  public boolean hasRack() {
    return this.hasRack;
  }

  /**
   * Set if this node has a defined rack
   *
   * @param hasRack if this node has a defined rack
   * @return  current instance of the class to be fluent
   */
  public Node setHasRack(boolean hasRack) {
    this.hasRack = hasRack;
    return this;
  }

  /**
   * The host name for this node
   *
   * @return
   */
  public String getHost() {
    return this.host;
  }

  /**
   * Set the host name for this node
   *
   * @param host  the host name for this node
   * @return  current instance of the class to be fluent
   */
  public Node setHost(String host) {
    this.host = host;
    return this;
  }

  /**
   * The node id of this node
   *
   * @return
   */
  public int getId() {
    return this.id;
  }

  /**
   * Set the node id of this node
   *
   * @param id  the node id of this node
   * @return  current instance of the class to be fluent
   */
  public Node setId(int id) {
    this.id = id;
    return this;
  }

  /**
   * String representation of the node id
   *
   * @return
   */
  public String getIdString() {
    return this.idString;
  }

  /**
   * Set the string representation of the node id
   *
   * @param idString  String representation of the node id
   * @return  current instance of the class to be fluent
   */
  public Node setIdString(String idString) {
    this.idString = idString;
    return this;
  }

  /**
   * Check whether this node is empty, which may be the case if noNode() is used as a placeholder in a response payload with an error
   *
   * @return
   */
  public boolean isEmpty() {
    return this.isEmpty;
  }

  /**
   * Set if this node is empty
   *
   * @param isEmpty if this node is empty
   * @return  current instance of the class to be fluent
   */
  public Node setIsEmpty(boolean isEmpty) {
    this.isEmpty = isEmpty;
    return this;
  }

  /**
   * The port for this node
   *
   * @return
   */
  public int getPort() {
    return this.port;
  }

  /**
   * Set the port for this node
   *
   * @param port  the port for this node
   * @return  current instance of the class to be fluent
   */
  public Node setPort(int port) {
    this.port = port;
    return this;
  }

  /**
   * The rack for this node
   *
   * @return
   */
  public String rack() {
    return this.rack;
  }

  /**
   * Set the rack for this node
   *
   * @param rack  the rack for this node
   * @return  current instance of the class to be fluent
   */
  public Node setRack(String rack) {
    this.rack = rack;
    return this;
  }

  /**
   * Convert object to JSON representation
   *
   * @return  JSON representation
   */
  public JsonObject toJson() {
    JsonObject jsonObject = new JsonObject();

    jsonObject
      .put("hasRack", this.hasRack)
      .put("host", this.host)
      .put("id", this.id)
      .put("idString", this.idString)
      .put("isEmpty", this.isEmpty)
      .put("port", this.port)
      .put("rack", this.rack);

    return jsonObject;
  }
}
