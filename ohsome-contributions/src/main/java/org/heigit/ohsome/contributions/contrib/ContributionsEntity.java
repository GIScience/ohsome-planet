package org.heigit.ohsome.contributions.contrib;

import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import org.heigit.ohsome.osm.OSMEntity;
import org.heigit.ohsome.osm.OSMId;

import java.time.Instant;
import java.util.*;
import java.util.function.Function;

import static java.util.Comparator.comparing;
import static java.util.Optional.ofNullable;


public class ContributionsEntity<T extends OSMEntity> extends AbstractContributions {

  private final PeekingIterator<T> majorVersions;
  protected T major;
  protected Instant timestamp;

  protected Map<OSMId, Contributions> oshContributions = new HashMap<>();
  protected Map<OSMId, Contributions> active = new HashMap<>();
  protected PriorityQueue<Contributions> queue = new PriorityQueue<>(
      comparing(this::timestamp).thenComparing(this::changeset));

  protected final Function<OSMId, Contributions> memberContributions;

  protected List<Contribution.ContribMember> members;
  private long changeset;
  private int userId;
  private String user;

  Instant timestamp(Contributions contributions) {
    if (contributions == null || !contributions.hasNext()) {
      return Instant.MAX;
    }
    return contributions.peek().timestamp();
  }

  long changeset(Contributions contributions) {
    if (contributions == null || !contributions.hasNext()) {
      return Long.MAX_VALUE;
    }
    return contributions.peek().changeset();
  }

  int userId(Contributions contributions) {
    if (contributions == null || !contributions.hasNext()) {
      return Integer.MAX_VALUE;
    }
    return contributions.peek().userId();
  }

  String user(Contributions contributions) {
    if (contributions == null || !contributions.hasNext()) {
      return "";
    }
    return contributions.peek().user();
  }

  public ContributionsEntity(List<T> osh, Function<OSMId, Contributions> memberContributions) {
    super(osh.getFirst().osmId());
    this.majorVersions = Iterators.peekingIterator(osh.iterator());
    this.memberContributions = memberContributions;
    initNextMajorVersion();
  }

  private void initNextMajorVersion() {
    this.major = majorVersions.hasNext() ? majorVersions.next() : null;
    if (major != null) {
      this.timestamp = major.timestamp();
      this.changeset = major.changeset();
      this.userId = major.userId();
      this.user = major.user();
      this.active.clear();
      this.queue.clear();
      this.members = initMembers();
    }
  }

  private List<Contribution.ContribMember> initMembers() {
    var majorMembers = major.members();
    var mems = new ArrayList<Contribution.ContribMember>(majorMembers.size());

    for (var m : majorMembers) {
      var member = active.computeIfAbsent(m.osmId(),this::getOshContributions);
      while (member.hasNext() && (!member.peek().timestamp().isAfter(timestamp) || member.peek().changeset() == changeset)) {
        member.next();
      }
      mems.add(new Contribution.ContribMember(m.type(), m.id(), member.prev(), m.role()));
    }

    queue.addAll(active.values());
    return mems;
  }

  private Contributions getOshContributions(OSMId osmId) {
    return oshContributions.computeIfAbsent(osmId, this::getContributions);
  }

  private Contributions getContributions(OSMId osmId) {
    var contrib = memberContributions.apply(osmId);
    return contrib != null ? contrib : new EmptyContributions(osmId);
  }

  @Override
  protected Contribution computeNext() {
    if (major == null) {
      return null;
    }

    var contrib = new Contribution(timestamp, changeset, userId, user, major, members);

    var nextMajorTimestamp =
        majorVersions.hasNext() ? majorVersions.peek().timestamp() : Instant.MAX;

    timestamp = timestamp(queue.peek());
    changeset = changeset(queue.peek());
    userId = userId(queue.peek());
    user = user(queue.peek());

    while (!queue.isEmpty() && changeset(queue.peek()) == changeset && timestamp(
        queue.peek()).isBefore(nextMajorTimestamp)) {
      var member = ofNullable(queue.poll()).orElseThrow();
      timestamp = timestamp(member);
      if (member.hasNext()) {
        member.next();
      }
      queue.add(member);
    }

    if (timestamp.isBefore(nextMajorTimestamp)) {
      // we got a minor version
      var majorMembers = major.members();
      members = new ArrayList<>(majorMembers.size());
      for (var member : majorMembers) {
        var memberContribution = active.get(member.osmId());
        while (memberContribution.hasNext() && !memberContribution.peek().timestamp().isAfter(timestamp)
            && changeset(memberContribution) == changeset) {
          memberContribution.next();
        }
        members.add(new Contribution.ContribMember(member.type(), member.id(), memberContribution.prev(), member.role()));
      }
    } else {
      // next major version
      initNextMajorVersion();
    }
    return contrib;
  }

}
