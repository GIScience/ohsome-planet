package org.heigit.ohsome.osm.geometry;

import org.locationtech.jts.geom.Coordinate;

import java.util.ArrayList;
import java.util.List;

class Segment {
  private List<Coordinate> coordinates;
  long wayId;
  private boolean isReversed = false;

  public Segment(List<Coordinate> coordinates, long wayId) {
    this.coordinates = new ArrayList<>(coordinates);
    this.wayId = wayId;
  }

  public void reverse() {
    this.isReversed = !this.isReversed;
  }

  public Coordinate getFirstCoordinate() {
    return isReversed ? this.coordinates.getLast() : this.coordinates.getFirst();
  }

  public Coordinate getLastCoordinate() {
    return isReversed ?  this.coordinates.getFirst() : this.coordinates.getLast();
  }

  public Coordinate getOtherCoordinate(Coordinate nodeId) {
    return this.getFirstCoordinate().equals2D(nodeId) ? this.getLastCoordinate() : this.getFirstCoordinate();
  }

  public Segment setFirstCoordinate(Coordinate nodeId) {
    if (!this.getFirstCoordinate().equals2D(nodeId)) reverse();
    return this;
  }

  public List<Coordinate> toCoordinates() {
    return getCoordinates();
  }

  public List<Coordinate> getCoordinates() {
    return isReversed ? coordinates.reversed() : coordinates;
  }

  @Override
  public int hashCode() {
    int h = 0;

    for (var coordinate : coordinates) {
      if (coordinate != null) {
        h += coordinate.hashCode();
      }
    }

    return h;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    Segment other = (Segment) obj;
    return isSame(other);
  }

  public boolean isSame(Segment other) {
    List<Coordinate> otherNodes = new ArrayList<>(other.getCoordinates());
    if (coordinates.size() != otherNodes.size()) return false;
    if (!coordinates.getFirst().equals2D(otherNodes.getFirst())) {
      if (!coordinates.getFirst().equals2D(otherNodes.getLast())) {
        return false;
      }
      if (!coordinates.getLast().equals2D(otherNodes.getFirst())) {
        return false;
      }
      otherNodes = otherNodes.reversed();
    }
    if (!coordinates.getLast().equals2D(otherNodes.getLast())) {
      return false;
    }
    for (int i = 1; i < coordinates.size() - 1; i++) {
      if (!coordinates.get(i).equals2D(otherNodes.get(i))) {
        return false;
      }
    }
    return true;
  }

}
