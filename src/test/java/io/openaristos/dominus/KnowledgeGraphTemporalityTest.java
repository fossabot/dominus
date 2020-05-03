package io.openaristos.dominus;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Range;
import com.google.common.collect.TreeRangeSet;
import io.openaristos.dominus.core.graph.dsl.DominusTraversalSource;
import io.openaristos.dominus.core.graph.dsl.GremlinUtils;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.locationtech.jts.util.Assert;

import java.util.List;

@SuppressWarnings("UnstableApiUsage")
@RunWith(JUnit4.class)
public class KnowledgeGraphTemporalityTest {

  @Test
  public void canTraverseTemporaly() {
    final Graph graph = TinkerGraph.open();

    final DominusTraversalSource g = graph.traversal(DominusTraversalSource.class);

    g.addV("entity").property("uid", "uida").iterate();

    g.addV("entity").property("uid", "uidb").iterate();

    g.addV("entity").property("uid", "uidc").iterate();

    g.addV("entity").property("uid", "uidd").iterate();

    // 1. add entity a, b. c
    // 2. relate a -> b with intervals [1, 5]
    // 3. relate b -> c with intervals [6, 7]
    // 4. relate b -> d with intervals [1, 4]

    g.V()
        .has("uid", "uida")
        .as("a")
        .V()
        .has("uid", "uidb")
        .as("b")
        .addE("relatedTo")
        .from("a")
        .to("b")
        .property("effectiveDating", TreeRangeSet.create(ImmutableSet.of(Range.closed(1L, 5L))))
        .V()
        .has("uid", "uidc")
        .as("c")
        .addE("relatedTo")
        .from("b")
        .to("c")
        .property("effectiveDating", TreeRangeSet.create(ImmutableSet.of(Range.closed(6L, 7L))))
        .V()
        .has("uid", "uidd")
        .as("d")
        .addE("relatedTo")
        .from("b")
        .to("d")
        .property("effectiveDating", TreeRangeSet.create(ImmutableSet.of(Range.closed(1L, 4L))))
        .iterate();

    // traverse from a -> b -> [c, d] with temporal awareness

    final List<Vertex> v1 =
        GremlinUtils.temporalTraversal(g)
            .V()
            .has("uid", "uida")
            .outT("relatedTo")
            .outT("relatedTo")
            .toList();

    // we should expect 1 overlap with d.
    Assert.equals(1, v1.size());

    // modify the edge of b -> c to [5, 7]
    g.V()
        .has("uid", "uidb")
        .as("b")
        .outE("relatedTo")
        .as("e")
        .inV()
        .has("uid", "uidc")
        .select("e")
        .property("effectiveDating", TreeRangeSet.create(ImmutableSet.of(Range.closed(5L, 7L))))
        .iterate();

    // repeat the traversal

    final List<Path> v2 =
        GremlinUtils.temporalTraversal(g)
            .V()
            .has("uid", "uida")
            .outT("relatedTo")
            .outT("relatedTo")
            .path()
            .by()
            .by("temporality")
            .toList();

    // we should see a node, because [1, 5] and [5,7] overlap
    Assert.equals(2, v2.size());
  }

  @Test
  public void canTraverseNestedCollection() {
    final Graph graph = TinkerGraph.open();
    final DominusTraversalSource g = graph.traversal(DominusTraversalSource.class);

    // A -> [1,100] -> B (InstrumentIssue/ShareClass to Fund)
    // B -> [3,4] -> [C,D] (Fund to InstrumentIssue/Members)
    // B -> [[1,2],[4,5]] -> [E] (Fund to InstrumentIssue/Members)
    // E -> [1,100] -> F (InstrumentIssue/ShareClass to Fund)
    // F -> [1,5] -> [G] (Fund to InstrumentIssue/Members)
    // F -> [3,4] -> [H] (Fund to InstrumentIssue/Members)
    // H -> [3,4] -> [I] (InstrumentIssue to InstrumentRegion)
    // E -> [3,4] -> [J] (InstrumentIssue to InstrumentRegion)

    //   AB     BC     BD     BE     EF     FG     FH    HI     EJ
    // 1 x                    x      x      x
    // 2 x                           x      x
    // 3 x      x      x             x      x      x     x      x
    // 4 x                    x      x      x
    // 5 x                           x

    // create knowledge entities
    // A
    g.addV("instrument_issue").property("fsym_security_id", "A").property("uid", "A").iterate();

    // B
    g.addV("fund").property("factset_fund_id", "B").property("uid", "B").iterate();

    // C
    g.addV("instrument_issue").property("fsym_security_id", "C").property("uid", "C").iterate();

    // D
    g.addV("instrument_issue").property("fsym_security_id", "D").property("uid", "D").iterate();

    // E
    g.addV("instrument_issue").property("fsym_security_id", "E").property("uid", "E").iterate();

    // F
    g.addV("fund").property("factset_fund_id", "F").property("uid", "F").iterate();

    // G
    g.addV("instrument_issue").property("fsym_security_id", "G").property("uid", "G").iterate();

    // H
    g.addV("instrument_issue").property("fsym_security_id", "H").property("uid", "H").iterate();

    // I
    g.addV("instrument_region").property("fsym_region_id", "I").property("uid", "I").iterate();

    // J
    g.addV("instrument_region").property("fsym_region_id", "J").property("uid", "J").iterate();

    // add properties
    g.V()
        .has("fsym_security_id", "A")
        .as("A")
        .V()
        .has("factset_fund_id", "B")
        .as("B")
        .addE("instrumentIssueIsShareClassOfFund")
        .from("A")
        .to("B")
        .property("effectiveDating", TreeRangeSet.create(ImmutableSet.of(Range.closed(1L, 100L))))
        .V()
        .has("fsym_security_id", "C")
        .as("C")
        .addE("instrumentIssueIsConstituentOfFund")
        .from("B")
        .to("C")
        .property("effectiveDating", TreeRangeSet.create(ImmutableSet.of(Range.closed(3L, 4L))))
        .V()
        .has("fsym_security_id", "D")
        .as("D")
        .addE("instrumentIssueIsConstituentOfFund")
        .from("B")
        .to("C")
        .property("effectiveDating", TreeRangeSet.create(ImmutableSet.of(Range.closed(3L, 4L))))
        .V()
        .has("fsym_security_id", "E")
        .as("E")
        .addE("instrumentIssueIsConstituentOfFund")
        .from("B")
        .to("E")
        .property(
            "effectiveDating",
            TreeRangeSet.create(ImmutableSet.of(Range.closed(1L, 2L), Range.closed(4L, 5L))))
        .V()
        .has("factset_fund_id", "F")
        .as("F")
        .addE("instrumentIssueIsShareClassOfFund")
        .from("E")
        .to("F")
        .property("effectiveDating", TreeRangeSet.create(ImmutableSet.of(Range.closed(1L, 100L))))
        .V()
        .has("fsym_security_id", "G")
        .as("G")
        .addE("instrumentIssueIsConstituentOfFund")
        .from("F")
        .to("G")
        .property("effectiveDating", TreeRangeSet.create(ImmutableSet.of(Range.closed(1L, 5L))))
        .V()
        .has("fsym_security_id", "H")
        .as("H")
        .addE("instrumentIssueIsConstituentOfFund")
        .from("F")
        .to("H")
        .property("effectiveDating", TreeRangeSet.create(ImmutableSet.of(Range.closed(3L, 4L))))
        .V()
        .has("fsym_region_id", "I")
        .as("I")
        .addE("issuedRegionalInstrument")
        .from("H")
        .to("I")
        .property("effectiveDating", TreeRangeSet.create(ImmutableSet.of(Range.closed(3L, 4L))))
        .V()
        .has("fsym_region_id", "J")
        .as("J")
        .addE("issuedRegionalInstrument")
        .from("E")
        .to("J")
        .property("effectiveDating", TreeRangeSet.create(ImmutableSet.of(Range.closed(3L, 4L))))
        .iterate();

    // try to traverse A -> B -> C, expecting 0, because C cannot be reached during [1,2]
    Assert.equals(
        0,
        GremlinUtils.temporalTraversal(g, 1L, 2L)
            .V()
            .has("fsym_security_id", "A")
            .outT("instrumentIssueIsShareClassOfFund")
            .has("factset_fund_id", "B")
            .outT("instrumentIssueIsConstituentOfFund")
            .has("fsym_security_id", "C")
            .toList()
            .size());

    // traverse A -> B -> C, expecting 2, since time is [3,4]
    Assert.equals(
        2,
        GremlinUtils.temporalTraversal(g, 3L, 4L)
            .V()
            .has("fsym_security_id", "A")
            .outT("instrumentIssueIsShareClassOfFund")
            .has("factset_fund_id", "B")
            .outT("instrumentIssueIsConstituentOfFund")
            .has("fsym_security_id", "C")
            .toList()
            .size());

    // traverse A -> B -> E -> F -> H, expecting 1, since time is [1,5]
    Assert.equals(
        1,
        GremlinUtils.temporalTraversal(g, 1L, 5L)
            .V()
            .has("fsym_security_id", "A")
            .outT("instrumentIssueIsShareClassOfFund")
            .has("factset_fund_id", "B")
            .outT("instrumentIssueIsConstituentOfFund")
            .has("fsym_security_id", "E")
            .outT("instrumentIssueIsShareClassOfFund")
            .has("factset_fund_id", "F")
            .outT("instrumentIssueIsConstituentOfFund")
            .has("fsym_security_id", "H")
            .toList()
            .size());

    // traverse A -> B -> E -> F -> H, expecting 0, since time is [1,2], and H doesn't exist then
    Assert.equals(
        0,
        GremlinUtils.temporalTraversal(g, 1L, 2L)
            .V()
            .has("fsym_security_id", "A")
            .outT("instrumentIssueIsShareClassOfFund")
            .has("factset_fund_id", "B")
            .outT("instrumentIssueIsConstituentOfFund")
            .has("fsym_security_id", "E")
            .outT("instrumentIssueIsShareClassOfFund")
            .has("factset_fund_id", "F")
            .outT("instrumentIssueIsConstituentOfFund")
            .has("fsym_security_id", "H")
            .toList()
            .size());
  }
}
