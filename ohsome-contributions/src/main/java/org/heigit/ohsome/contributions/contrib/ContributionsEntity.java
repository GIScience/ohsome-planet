package org.heigit.ohsome.contributions.contrib;

import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import org.heigit.ohsome.osm.OSMEntity;
import org.heigit.ohsome.osm.OSMType;

import java.time.Instant;
import java.util.*;

import static java.util.Comparator.comparing;
import static java.util.Optional.ofNullable;


public class ContributionsEntity<T extends OSMEntity> extends AbstractContributions {

    public interface MemberOfFunction {
        Contributions apply(OSMType type, long id);
    }

    private final PeekingIterator<T> majorVersions;
    protected T major;
    protected Instant timestamp;

    protected Map<OSMType, Long2ObjectMap<Contributions>> oshContributions = new EnumMap<>(OSMType.class);


    protected Map<OSMType, Long2ObjectMap<Contributions>> active = new EnumMap<>(OSMType.class);
    protected PriorityQueue<Contributions> queue = new PriorityQueue<>(
            comparing(this::timestamp).thenComparing(this::changeset));

    protected final MemberOfFunction memberContributions;

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

    public ContributionsEntity(OSMType type, long id, List<T> osh, MemberOfFunction memberContributions) {
        super(type, id);
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
        var mems = new ArrayList<Contribution.ContribMember>(major.memberSize());

        major.members((mType, mId, mRole) -> {
                    var member = active.computeIfAbsent(mType, t -> new Long2ObjectOpenHashMap<>())
                            .computeIfAbsent(mId, id -> getOshContributions(mType, mId));
                    while (member.hasNext() && (!member.peek().timestamp().isAfter(timestamp) || member.peek().changeset() == changeset)) {
                        member.next();
                    }
                    mems.add(new Contribution.ContribMember(mType, mId, member.prev(), mRole));
                }
        );

        active.forEach((type, member) -> queue.addAll(member.values()));

        return mems;
    }

    private Contributions getOshContributions(OSMType type, long id) {
        return oshContributions.computeIfAbsent(type, t -> new Long2ObjectOpenHashMap<>())
                .computeIfAbsent(id, x -> getContributions(type, id));
    }


    private Contributions getContributions(OSMType type, long id) {
        var contrib = memberContributions.apply(type, id);
        return contrib != null ? contrib : new EmptyContributions(type, id);
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
            var mems = new ArrayList<Contribution.ContribMember>(major.memberSize());
            major.members((mType, mId, mRole) -> {
                var memberContribution = active.get(mType).get(mId);
                while (memberContribution.hasNext() && !memberContribution.peek().timestamp().isAfter(timestamp)
                       && changeset(memberContribution) == changeset) {
                    memberContribution.next();
                }
                mems.add(new Contribution.ContribMember(mType, mId, memberContribution.prev(), mRole));
            });
            members = mems;


        } else {
            // next major version
            initNextMajorVersion();
        }
        return contrib;
    }

}
