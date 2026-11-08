package io.github.deripas.taskgrid.mailbox.pekko;

import io.github.deripas.taskgrid.mailbox.Box;
import io.github.deripas.taskgrid.mailbox.Reply;
import io.github.deripas.taskgrid.mailbox.SupersededException;
import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.actor.typed.javadsl.AbstractBehavior;
import org.apache.pekko.actor.typed.javadsl.ActorContext;
import org.apache.pekko.actor.typed.javadsl.Behaviors;
import org.apache.pekko.actor.typed.javadsl.Receive;
import org.apache.pekko.cluster.sharding.typed.javadsl.EntityRef;

import java.time.Duration;
import java.util.concurrent.Flow;

/**
 * A subscription of a box on the node of its subscriber: an actor between the {@link Flow.Subscriber} and the
 * {@link BoxEntity} on any node. It passes the demand of the subscriber to the box and the results of the box to the
 * subscriber, on its own thread, one at a time. The box does not persist its subscriber: a heartbeat finds out that
 * the box lost it (restarted, moved to another node) and subscribes again with the demand not served yet; the box
 * then sends again what is not acknowledged.
 */
final class BoxSubscriber extends AbstractBehavior<BoxSubscriber.Message> {
    /**
     * Messages of the subscriber; those of the box come from other nodes: serialized with Jackson (reference.conf).
     */
    sealed interface Message {
    }

    /**
     * A result of the box.
     */
    record Deliver(Reply reply) implements Message {
    }

    /**
     * Another subscriber has the box.
     */
    record Superseded() implements Message {
    }

    /**
     * The box does not know this subscriber.
     */
    record Unknown() implements Message {
    }

    /**
     * Demand of the subscriber, from its {@link Flow.Subscription}.
     */
    record Request(long n) implements Message {
    }

    record Cancel() implements Message {
    }

    record Beat() implements Message {
    }

    private final EntityRef<BoxEntity.Command> entity;
    private final Box box;
    private final Flow.Subscriber<? super Reply> subscriber;
    /**
     * Requested by the subscriber and not delivered yet.
     */
    private long outstanding;

    private BoxSubscriber(
        ActorContext<Message> context,
        EntityRef<BoxEntity.Command> entity,
        Box box,
        Flow.Subscriber<? super Reply> subscriber
    ) {
        super(context);
        this.entity = entity;
        this.box = box;
        this.subscriber = subscriber;
    }

    static Behavior<Message> create(
        EntityRef<BoxEntity.Command> entity,
        Box box,
        Flow.Subscriber<? super Reply> subscriber,
        Duration heartbeat
    ) {
        return Behaviors.setup(context -> Behaviors.withTimers(timers -> {
            timers.startTimerWithFixedDelay(new Beat(), heartbeat);
            entity.tell(new BoxEntity.Subscribe(context.getSelf(), 0));
            subscriber.onSubscribe(new Flow.Subscription() {
                @Override
                public void request(long n) {
                    context.getSelf().tell(new Request(n));
                }

                @Override
                public void cancel() {
                    context.getSelf().tell(new Cancel());
                }
            });
            return new BoxSubscriber(context, entity, box, subscriber);
        }));
    }

    @Override
    public Receive<Message> createReceive() {
        return newReceiveBuilder()
            .onMessage(Request.class, this::request)
            .onMessage(Deliver.class, deliver -> {
                if (outstanding != Long.MAX_VALUE) {
                    outstanding--;
                }
                subscriber.onNext(deliver.reply());
                return this;
            })
            .onMessage(Unknown.class, unknown -> {
                entity.tell(new BoxEntity.Subscribe(getContext().getSelf(), outstanding));
                return this;
            })
            .onMessage(Beat.class, beat -> {
                entity.tell(new BoxEntity.Heartbeat(getContext().getSelf()));
                return this;
            })
            .onMessage(Superseded.class, superseded -> {
                subscriber.onError(new SupersededException(box));
                return Behaviors.stopped();
            })
            .onMessage(Cancel.class, cancel -> {
                entity.tell(new BoxEntity.Unsubscribe(getContext().getSelf()));
                return Behaviors.stopped();
            })
            .build();
    }

    private Behavior<Message> request(Request request) {
        if (request.n() <= 0) {
            entity.tell(new BoxEntity.Unsubscribe(getContext().getSelf()));
            subscriber.onError(new IllegalArgumentException("request must be positive: " + request.n()));
            return Behaviors.stopped();
        }
        outstanding = outstanding + request.n() < 0 ? Long.MAX_VALUE : outstanding + request.n();
        entity.tell(new BoxEntity.Demand(getContext().getSelf(), request.n()));
        return this;
    }
}
