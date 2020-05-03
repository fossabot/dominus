package io.openaristos.dominus;

import com.google.common.collect.*;
import io.openaristos.dominus.core.*;
import io.openaristos.dominus.core.internal.LocalMemoryEntityUniverse;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

@RunWith(JUnit4.class)
public class LocalMemoryEntityUniverseTest {

  static final LocalEntityModel.Matcher EQUALITY_MATCHER =
      LocalEntityModel.Matcher.of("equality", (x) -> x.getSource().equalsIgnoreCase(x.getTarget()));

  static final String PERSPECTIVE_A = "perspective_a";

  @BeforeClass
  public static void beforeAllTestMethods() {}

  static RangeSet<Long> createRangeSet(List<Long> intervals) {
    long begin = 0;

    final RangeSet<Long> result = TreeRangeSet.create();

    for (long current : intervals) {
      result.add(Range.closed(begin, current));
      begin = current;
    }

    return result;
  }

  static RangeSet<Long> perpetualRangeSet() {
    return TreeRangeSet.create(ImmutableSet.of(Range.openClosed(0L, Long.MAX_VALUE)));
  }

  @Test
  public void testSimpleMatching() {
    final LocalEntityModel.Attribute name =
        LocalEntityModel.Attribute.of(
            "name", LocalEntityModel.AttributeType.STRING, EQUALITY_MATCHER);
    final LocalEntityModel.Attribute email =
        LocalEntityModel.Attribute.of(
            "email", LocalEntityModel.AttributeType.STRING, EQUALITY_MATCHER);
    final LocalEntityModel.Attribute dob =
        LocalEntityModel.Attribute.of(
            "dob", LocalEntityModel.AttributeType.STRING, EQUALITY_MATCHER);
    final LocalEntityModel.Attribute uid =
        LocalEntityModel.Attribute.of(
            "uid", LocalEntityModel.AttributeType.STRING, EQUALITY_MATCHER);

    final LocalEntityModel personEntityModel =
        LocalEntityModel.of(
            ImmutableSet.of(name, email, dob, uid),
            ImmutableSet.of(
                LocalEntityModel.Resolver.of(
                    "name_email_dob", ImmutableSet.of(name, email, dob), 3),
                LocalEntityModel.Resolver.of("name_email", ImmutableSet.of(name, email), 2),
                LocalEntityModel.Resolver.of("email", ImmutableSet.of(email), 1),
                LocalEntityModel.Resolver.of("name", ImmutableSet.of(name), 0)));

    final EntityType personEntityType = EntityType.of("person", personEntityModel);

    final LocalMemoryEntityUniverse universe = LocalMemoryEntityUniverse.of(personEntityType);

    final LocalMasterableEntityIdentity id1 =
        getRandomIdentity(personEntityType, PERSPECTIVE_A, uid);

    final LocalMasterableEntity me1 =
        LocalMasterableEntity.of(
            personEntityType,
            id1,
            ImmutableMap.of(
                LocalMasterableEntityDescriptor.of(name, "isaac elbaz"),
                createRangeSet(ImmutableList.of(0L))));

    final LocalMasterableEntityIdentity id2 =
        getRandomIdentity(personEntityType, PERSPECTIVE_A, uid);

    final LocalMasterableEntity me2 =
        LocalMasterableEntity.of(
            personEntityType,
            id2,
            ImmutableMap.of(
                LocalMasterableEntityDescriptor.of(name, "isaac elbaz"),
                createRangeSet(ImmutableList.of(0L)),
                LocalMasterableEntityDescriptor.of(email, "isaac_elbaz@openaristos.io"),
                createRangeSet(ImmutableList.of(0L))));

    universe.append(me1);
    universe.append(me2);

    final Map<LocalMasterEntity, Set<LocalMasterEntityMembership>> q1 =
        universe.resolve(ResolveQuery.of(ImmutableMap.of(name, "isaac elbaz")));

    Assert.assertEquals(q1.size(), 1);

    final LocalMasterEntity mek1 = q1.keySet().iterator().next();

    Assert.assertTrue(mek1.getMemberIdentities().keySet().containsAll(ImmutableSet.of(id1, id2)));

    final LocalMasterableEntityIdentity id3 =
        getRandomIdentity(personEntityType, PERSPECTIVE_A, uid);
    final LocalMasterableEntity me3 =
        LocalMasterableEntity.of(
            personEntityType,
            id3,
            ImmutableMap.of(
                LocalMasterableEntityDescriptor.of(email, "isaac_elbaz@openaristos.io"),
                createRangeSet(ImmutableList.of(0L)),
                LocalMasterableEntityDescriptor.of(name, "doug"),
                createRangeSet(ImmutableList.of(0L))));

    universe.append(me3);

    final Map<LocalMasterEntity, Set<LocalMasterEntityMembership>> q2 =
        universe.resolve(ResolveQuery.of(ImmutableMap.of(email, "isaac_elbaz@openaristos.io")));
    Assert.assertEquals(q2.size(), 1);

    final LocalMasterEntity mek2 = q2.keySet().iterator().next();

    Assert.assertTrue(mek2.getMemberIdentities().keySet().containsAll(ImmutableSet.of(id1, id2, id3)));
  }

  private LocalMasterableEntityIdentity getRandomIdentity(
      EntityType entityType, String perspective, LocalEntityModel.Attribute attr) {
    final String randomValue = UUID.randomUUID().toString();

    return LocalMasterableEntityIdentity.of(
        randomValue,
        entityType,
        perspective,
        LocalMasterableEntityDescriptor.of(attr, randomValue));
  }

  private LocalMasterableEntity getRandomMasterableEntity(
      EntityType entityType, String perspective, LocalEntityModel.Attribute attr) {

    final String randomValue = UUID.randomUUID().toString();

    final LocalMasterableEntityIdentity identity =
        LocalMasterableEntityIdentity.of(
            randomValue,
            entityType,
            perspective,
            LocalMasterableEntityDescriptor.of(attr, randomValue));

    return LocalMasterableEntity.of(
        entityType,
        identity,
        ImmutableMap.of(
            LocalMasterableEntityDescriptor.of(attr, randomValue), perpetualRangeSet()));
  }

  @Test
  public void testFinancialSimpleMatching() {
    /*
    USE CASE:
    Match three equity instrument entities from three perspectives into a single knowledge entity.
    This use case represents "Everest Re Group Ltd."
     */

    // create attributes for entity model
    final LocalEntityModel.Attribute figi_composite =
        LocalEntityModel.Attribute.of(
            "figi_composite", LocalEntityModel.AttributeType.STRING, EQUALITY_MATCHER);
    final LocalEntityModel.Attribute axioma_id =
        LocalEntityModel.Attribute.of(
            "axioma_id", LocalEntityModel.AttributeType.STRING, EQUALITY_MATCHER);
    final LocalEntityModel.Attribute qaid =
        LocalEntityModel.Attribute.of(
            "qaid", LocalEntityModel.AttributeType.STRING, EQUALITY_MATCHER);
    final LocalEntityModel.Attribute sedol =
        LocalEntityModel.Attribute.of(
            "sedol", LocalEntityModel.AttributeType.STRING, EQUALITY_MATCHER);
    final LocalEntityModel.Attribute cusip =
        LocalEntityModel.Attribute.of(
            "cusip", LocalEntityModel.AttributeType.STRING, EQUALITY_MATCHER);
    final LocalEntityModel.Attribute country_of_trade =
        LocalEntityModel.Attribute.of(
            "country_of_trade", LocalEntityModel.AttributeType.STRING, EQUALITY_MATCHER);
    final LocalEntityModel.Attribute currency_of_trade =
        LocalEntityModel.Attribute.of(
            "currency_of_trade", LocalEntityModel.AttributeType.STRING, EQUALITY_MATCHER);
    final LocalEntityModel.Attribute uid =
        LocalEntityModel.Attribute.of(
            "uid", LocalEntityModel.AttributeType.STRING, EQUALITY_MATCHER);

    // create entity model
    final LocalEntityModel equityEntityModel =
        LocalEntityModel.of(
            ImmutableSet.of(
                figi_composite,
                axioma_id,
                qaid,
                sedol,
                cusip,
                country_of_trade,
                currency_of_trade,
                uid),
            ImmutableSet.of(
                LocalEntityModel.Resolver.of(
                    "sedol_country_currency_and_cusip_country_currency",
                    ImmutableSet.of(
                        sedol,
                        country_of_trade,
                        currency_of_trade,
                        cusip,
                        country_of_trade,
                        currency_of_trade),
                    10),
                LocalEntityModel.Resolver.of(
                    "sedol_country_currency",
                    ImmutableSet.of(sedol, country_of_trade, currency_of_trade),
                    9),
                LocalEntityModel.Resolver.of(
                    "sedol_country", ImmutableSet.of(sedol, country_of_trade), 8),
                LocalEntityModel.Resolver.of(
                    "cusip_country_currency",
                    ImmutableSet.of(cusip, country_of_trade, currency_of_trade),
                    7),
                LocalEntityModel.Resolver.of(
                    "cusip_country",
                    ImmutableSet.of(cusip, country_of_trade, currency_of_trade),
                    6)));

    // define entity type
    final EntityType equityEntityType = EntityType.of("equity_instrument", equityEntityModel);

    // define perspectives
    final String axioma = "axioma";
    final String refinitiv = "refinitiv";
    final String bloomberg = "bloomberg";

    // create entity universe and add entity
    final LocalMemoryEntityUniverse universe = LocalMemoryEntityUniverse.of(equityEntityType);

    // create entity for first perspective
    final LocalMasterableEntityIdentity id1 = getRandomIdentity(equityEntityType, axioma, uid);

    final LocalMasterableEntity e1 =
        LocalMasterableEntity.of(
            equityEntityType,
            id1,
            ImmutableMap.of(
                LocalMasterableEntityDescriptor.of(axioma_id, "XD6RCVDW6"),
                createRangeSet(ImmutableList.of(0L)),
                LocalMasterableEntityDescriptor.of(sedol, "2556868"),
                createRangeSet(ImmutableList.of(0L)),
                LocalMasterableEntityDescriptor.of(cusip, "G3223R10"),
                createRangeSet(ImmutableList.of(0L)),
                LocalMasterableEntityDescriptor.of(country_of_trade, "US"),
                createRangeSet(ImmutableList.of(0L)),
                LocalMasterableEntityDescriptor.of(currency_of_trade, "USD"),
                createRangeSet(ImmutableList.of(0L))));

    universe.append(e1);

    // create entity for second perspective
    final LocalMasterableEntityIdentity id2 = getRandomIdentity(equityEntityType, refinitiv, uid);

    final LocalMasterableEntity e2 =
        LocalMasterableEntity.of(
            equityEntityType,
            id2,
            ImmutableMap.of(
                LocalMasterableEntityDescriptor.of(qaid, "RE-US"),
                createRangeSet(ImmutableList.of(0L)),
                LocalMasterableEntityDescriptor.of(sedol, "2556868"),
                createRangeSet(ImmutableList.of(0L)),
                LocalMasterableEntityDescriptor.of(cusip, "G3223R10"),
                createRangeSet(ImmutableList.of(0L)),
                LocalMasterableEntityDescriptor.of(country_of_trade, "US"),
                createRangeSet(ImmutableList.of(0L)),
                LocalMasterableEntityDescriptor.of(currency_of_trade, "USD"),
                createRangeSet(ImmutableList.of(0L))));

    universe.append(e2);

    // resolve query
    final Map<LocalMasterEntity, Set<LocalMasterEntityMembership>> q1 =
        universe.resolve(ResolveQuery.of(ImmutableMap.of(sedol, "2556868")));

    Assert.assertEquals(q1.size(), 1);
    Assert.assertEquals(q1.keySet().iterator().next().getMemberIdentities().size(), 2);

    final LocalMasterEntity ke1 = q1.keySet().iterator().next();

    Assert.assertTrue(ke1.getMemberIdentities().keySet().containsAll(ImmutableSet.of(id1, id2)));

    // create entity for third perspective
    final LocalMasterableEntityIdentity id3 = getRandomIdentity(equityEntityType, bloomberg, uid);

    final LocalMasterableEntity e3 =
        LocalMasterableEntity.of(
            equityEntityType,
            id3,
            ImmutableMap.of(
                LocalMasterableEntityDescriptor.of(figi_composite, "BBG000C1XVK6"),
                createRangeSet(ImmutableList.of(0L)),
                LocalMasterableEntityDescriptor.of(sedol, "2556868"),
                createRangeSet(ImmutableList.of(0L)),
                LocalMasterableEntityDescriptor.of(country_of_trade, "US"),
                createRangeSet(ImmutableList.of(0L))));

    universe.append(e3);

    // resolve query
    final Map<LocalMasterEntity, Set<LocalMasterEntityMembership>> q2 =
        universe.resolve(ResolveQuery.of(ImmutableMap.of(sedol, "2556868")));
    Assert.assertEquals(q2.size(), 1);

    final LocalMasterEntity ke2 = q2.keySet().iterator().next();

    Assert.assertTrue(ke2.getMemberIdentities().keySet().containsAll(ImmutableSet.of(id1, id2, id3)));
  }

  @Test
  public void testQuickstartMatching() {
    /*
    USE CASE:
    Resolve two persons into one and combine their characteristics.
    */

    // define person attributes
    final LocalEntityModel.Attribute ssn =
        LocalEntityModel.Attribute.of(
            "ssn", LocalEntityModel.AttributeType.STRING, EQUALITY_MATCHER);
    final LocalEntityModel.Attribute real_id =
        LocalEntityModel.Attribute.of(
            "real_id", LocalEntityModel.AttributeType.STRING, EQUALITY_MATCHER);
    final LocalEntityModel.Attribute ny_drivers_id =
        LocalEntityModel.Attribute.of(
            "ny_drivers_id", LocalEntityModel.AttributeType.STRING, EQUALITY_MATCHER);
    final LocalEntityModel.Attribute ok_drivers_id =
        LocalEntityModel.Attribute.of(
            "ok_drivers_id", LocalEntityModel.AttributeType.STRING, EQUALITY_MATCHER);
    final LocalEntityModel.Attribute dob =
        LocalEntityModel.Attribute.of(
            "dob", LocalEntityModel.AttributeType.STRING, EQUALITY_MATCHER);
    final LocalEntityModel.Attribute name =
        LocalEntityModel.Attribute.of(
            "name", LocalEntityModel.AttributeType.STRING, EQUALITY_MATCHER);
    final LocalEntityModel.Attribute uid =
        LocalEntityModel.Attribute.of(
            "uid", LocalEntityModel.AttributeType.STRING, EQUALITY_MATCHER);

    // assign attributes and resolvers to entity model
    final LocalEntityModel personEntityModel =
        LocalEntityModel.of(
            ImmutableSet.of(ssn, real_id, ny_drivers_id, ok_drivers_id, dob, name, uid),
            ImmutableSet.of(
                LocalEntityModel.Resolver.of("ssn", ImmutableSet.of(ssn), 5),
                LocalEntityModel.Resolver.of("real_id", ImmutableSet.of(real_id), 4),
                LocalEntityModel.Resolver.of("ny_drivers_id", ImmutableSet.of(ny_drivers_id), 3),
                LocalEntityModel.Resolver.of("ok_drivers_id", ImmutableSet.of(ok_drivers_id), 2),
                LocalEntityModel.Resolver.of("name_dob", ImmutableSet.of(name, dob), 1)));

    // define entity type
    final EntityType personEntityType = EntityType.of("Person", personEntityModel);

    // define company attributes
    final LocalEntityModel.Attribute tax_id =
        LocalEntityModel.Attribute.of(
            "tax_id", LocalEntityModel.AttributeType.STRING, EQUALITY_MATCHER);
    final LocalEntityModel.Attribute company_name =
        LocalEntityModel.Attribute.of(
            "name", LocalEntityModel.AttributeType.STRING, EQUALITY_MATCHER);
    final LocalEntityModel.Attribute country =
        LocalEntityModel.Attribute.of(
            "country", LocalEntityModel.AttributeType.STRING, EQUALITY_MATCHER);

    // assign attributes and resolvers to entity model
    final LocalEntityModel companyEntityModel =
        LocalEntityModel.of(
            ImmutableSet.of(tax_id, name, country, uid),
            ImmutableSet.of(
                LocalEntityModel.Resolver.of("tax_id", ImmutableSet.of(tax_id), 5),
                LocalEntityModel.Resolver.of(
                    "name_country", ImmutableSet.of(company_name, country), 4),
                LocalEntityModel.Resolver.of("name", ImmutableSet.of(company_name), 3)));

    // define entity type
    final EntityType companyEntityType = EntityType.of("Company", companyEntityModel);

    // define perspectives
    final String nyRMV = "New York RMV";
    final String okRMV = "Oklahoma RMV";

    // create entity universe and add entity
    final LocalMemoryEntityUniverse personUniverse = LocalMemoryEntityUniverse.of(personEntityType);
    final LocalMemoryEntityUniverse companyUniverse =
        LocalMemoryEntityUniverse.of(companyEntityType);

    // create entity for first perspective
    final LocalMasterableEntityIdentity id1 = getRandomIdentity(personEntityType, nyRMV, uid);

    final LocalMasterableEntity e1 =
        LocalMasterableEntity.of(
            personEntityType,
            id1,
            ImmutableMap.of(
                LocalMasterableEntityDescriptor.of(ssn, "046-77-9267"),
                    TreeRangeSet.create(
                        ImmutableList.of(Range.closed(19930508000000L, 20101231000000L))),
                LocalMasterableEntityDescriptor.of(ny_drivers_id, "S67234519"),
                    TreeRangeSet.create(
                        ImmutableList.of(Range.closed(19930508000000L, 20101231000000L))),
                LocalMasterableEntityDescriptor.of(dob, "19750508"),
                    TreeRangeSet.create(
                        ImmutableList.of(Range.closed(19930508000000L, 20101231000000L))),
                LocalMasterableEntityDescriptor.of(name, "Ottilie Adamec"),
                    TreeRangeSet.create(
                        ImmutableList.of(Range.closed(19930508000000L, 20101231000000L)))));

    personUniverse.append(e1);

    // create entity for second perspective
    final LocalMasterableEntityIdentity id2 = getRandomIdentity(personEntityType, okRMV, uid);

    final LocalMasterableEntity e2 =
        LocalMasterableEntity.of(
            personEntityType,
            id2,
            ImmutableMap.of(
                LocalMasterableEntityDescriptor.of(ssn, "046-77-9267"),
                    TreeRangeSet.create(
                        ImmutableList.of(Range.closed(20101201000000L, Long.MAX_VALUE))),
                LocalMasterableEntityDescriptor.of(ok_drivers_id, "K143913841"),
                    TreeRangeSet.create(
                        ImmutableList.of(Range.closed(20101201000000L, Long.MAX_VALUE))),
                LocalMasterableEntityDescriptor.of(name, "Ottilie Bellove"),
                    TreeRangeSet.create(
                        ImmutableList.of(Range.closed(20101201000000L, Long.MAX_VALUE)))));

    personUniverse.append(e2);

    // resolve query
    final Map<LocalMasterEntity, Set<LocalMasterEntityMembership>> q1 =
        personUniverse.resolve(ResolveQuery.of(ImmutableMap.of(ssn, "046-77-9267")));
    Assert.assertEquals(q1.keySet().iterator().next().getDescriptors().size(), 5);

    final LocalMasterEntity ke1 = q1.keySet().iterator().next();

    Assert.assertTrue(ke1.getMemberIdentities().keySet().containsAll(ImmutableSet.of(id1, id2)));
  }

  @Test
  public void testFinancialTutorialMatching() {
    /*
    USE CASE:
    Match two equity instrument entities from two perspectives into a single knowledge entity.
    */

    // create attributes for entity model
    final LocalEntityModel.Attribute sedol =
        LocalEntityModel.Attribute.of(
            "sedol", LocalEntityModel.AttributeType.STRING, EQUALITY_MATCHER);
    final LocalEntityModel.Attribute isin =
        LocalEntityModel.Attribute.of(
            "isin", LocalEntityModel.AttributeType.STRING, EQUALITY_MATCHER);
    final LocalEntityModel.Attribute country =
        LocalEntityModel.Attribute.of(
            "country", LocalEntityModel.AttributeType.STRING, EQUALITY_MATCHER);
    final LocalEntityModel.Attribute currency =
        LocalEntityModel.Attribute.of(
            "currency", LocalEntityModel.AttributeType.STRING, EQUALITY_MATCHER);
    final LocalEntityModel.Attribute exchange =
        LocalEntityModel.Attribute.of(
            "exchange", LocalEntityModel.AttributeType.STRING, EQUALITY_MATCHER);
    final LocalEntityModel.Attribute ticker =
        LocalEntityModel.Attribute.of(
            "ticker", LocalEntityModel.AttributeType.STRING, EQUALITY_MATCHER);
    final LocalEntityModel.Attribute name =
        LocalEntityModel.Attribute.of(
            "name", LocalEntityModel.AttributeType.STRING, EQUALITY_MATCHER);
    final LocalEntityModel.Attribute figi_composite =
        LocalEntityModel.Attribute.of(
            "figi_composite", LocalEntityModel.AttributeType.STRING, EQUALITY_MATCHER);
    final LocalEntityModel.Attribute open_permid =
        LocalEntityModel.Attribute.of(
            "open_permid", LocalEntityModel.AttributeType.STRING, EQUALITY_MATCHER);
    final LocalEntityModel.Attribute ric =
        LocalEntityModel.Attribute.of(
            "ric", LocalEntityModel.AttributeType.STRING, EQUALITY_MATCHER);
    final LocalEntityModel.Attribute uid =
        LocalEntityModel.Attribute.of(
            "uid", LocalEntityModel.AttributeType.STRING, EQUALITY_MATCHER);

    // create entity model
    final LocalEntityModel equityEntityModel =
        LocalEntityModel.of(
            ImmutableSet.of(
                sedol,
                isin,
                country,
                currency,
                exchange,
                ticker,
                name,
                figi_composite,
                open_permid,
                ric,
                uid),
            ImmutableSet.of(
                LocalEntityModel.Resolver.of("sedol_country", ImmutableSet.of(sedol, country), 10),
                LocalEntityModel.Resolver.of(
                    "country_ticker", ImmutableSet.of(country, exchange, ticker), 9),
                LocalEntityModel.Resolver.of(
                    "figi_composite", ImmutableSet.of(figi_composite), 8)));

    // define entity type
    final EntityType equityEntityType = EntityType.of("equity_instrument", equityEntityModel);

    // define perspectives
    final String bloomberg = "Bloomberg OpenFigi";
    final String refinitiv = "Refinitiv OpenPermID";

    // create entity universe and add entity
    final LocalMemoryEntityUniverse universe = LocalMemoryEntityUniverse.of(equityEntityType);

    // create entity for first perspective
    final LocalMasterableEntityIdentity id1 = getRandomIdentity(equityEntityType, bloomberg, uid);

    final LocalMasterableEntity e1 =
        LocalMasterableEntity.of(
            equityEntityType,
            id1,
            ImmutableMap.of(
                LocalMasterableEntityDescriptor.of(sedol, "BSJC6M6"),
                defaultRangeSet(),
                LocalMasterableEntityDescriptor.of(country, "USA"),
                defaultRangeSet(),
                LocalMasterableEntityDescriptor.of(exchange, "XNYS"),
                defaultRangeSet(),
                LocalMasterableEntityDescriptor.of(ticker, "BRK.B"),
                defaultRangeSet(),
                LocalMasterableEntityDescriptor.of(figi_composite, "BBG00JRQRTM4"),
                defaultRangeSet()));

    final LocalMemoryEntityUniverse.AppendResult ar1 = universe.append(e1);

    // create entity for second perspective
    final LocalMasterableEntityIdentity id2 = getRandomIdentity(equityEntityType, refinitiv, uid);

    final LocalMasterableEntity e2 =
        LocalMasterableEntity.of(
            equityEntityType,
            id2,
            ImmutableMap.of(
                LocalMasterableEntityDescriptor.of(sedol, "BSJC6M6"),
                defaultRangeSet(),
                LocalMasterableEntityDescriptor.of(country, "USA"),
                defaultRangeSet(),
                LocalMasterableEntityDescriptor.of(exchange, "XNYS"),
                defaultRangeSet(),
                LocalMasterableEntityDescriptor.of(ticker, "BRK.B"),
                defaultRangeSet(),
                LocalMasterableEntityDescriptor.of(name, "Berkshire Hathaway Ord Shs Class B"),
                defaultRangeSet()));

    final LocalMemoryEntityUniverse.AppendResult ar2 = universe.append(e2);

    // resolve query
    final Map<LocalMasterEntity, Set<LocalMasterEntityMembership>> q1 =
        universe.resolve(ResolveQuery.of(ImmutableMap.of(figi_composite, "BBG00JRQRTM4")));

    Assert.assertEquals(q1.size(), 1);
    Assert.assertEquals(q1.keySet().iterator().next().getMemberIdentities().size(), 2);

    final LocalMasterEntity ke1 = q1.keySet().iterator().next();

    Assert.assertTrue(ke1.getMemberIdentities().keySet().containsAll(ImmutableSet.of(id1, id2)));
  }

  @Test
  public void testSecurityMatching() {
    /*
    USE CASE:
    Resolve two securities into one and combine their characteristics.
    */

    // define person attributes
    final LocalEntityModel.Attribute fsym_security_id =
        LocalEntityModel.Attribute.of(
            "fsym_security_id", LocalEntityModel.AttributeType.STRING, EQUALITY_MATCHER);
    final LocalEntityModel.Attribute cusip =
        LocalEntityModel.Attribute.of(
            "cusip", LocalEntityModel.AttributeType.STRING, EQUALITY_MATCHER);
    final LocalEntityModel.Attribute ciq_security_id =
        LocalEntityModel.Attribute.of(
            "ciq_security_id", LocalEntityModel.AttributeType.STRING, EQUALITY_MATCHER);
    final LocalEntityModel.Attribute type =
        LocalEntityModel.Attribute.of(
            "type", LocalEntityModel.AttributeType.STRING, EQUALITY_MATCHER);
    final LocalEntityModel.Attribute name =
        LocalEntityModel.Attribute.of(
            "name", LocalEntityModel.AttributeType.STRING, EQUALITY_MATCHER);
    final LocalEntityModel.Attribute uid =
        LocalEntityModel.Attribute.of(
            "uid", LocalEntityModel.AttributeType.STRING, EQUALITY_MATCHER);

    // assign attributes and resolvers to entity model
    final LocalEntityModel issueEntityModel =
        LocalEntityModel.of(
            ImmutableSet.of(fsym_security_id, ciq_security_id, cusip, type, name, uid),
            ImmutableSet.of(
                LocalEntityModel.Resolver.of(
                    "fsym_security_id", ImmutableSet.of(fsym_security_id), 5),
                LocalEntityModel.Resolver.of("cusip", ImmutableSet.of(cusip), 4)));

    // define entity type
    final EntityType issueEntityType = EntityType.of("instrument_issue", issueEntityModel);

    // define perspectives
    final String factset = "FactSet";
    final String ciq = "CIQ";

    // create entity universe and add entity
    final LocalMemoryEntityUniverse personUniverse = LocalMemoryEntityUniverse.of(issueEntityType);

    // create entity for first perspective
    final LocalMasterableEntityIdentity id1 = getRandomIdentity(issueEntityType, factset, uid);

    final LocalMasterableEntity e1 =
        LocalMasterableEntity.of(
            issueEntityType,
            id1,
            ImmutableMap.of(
                LocalMasterableEntityDescriptor.of(fsym_security_id, "DWT1TS-S"),
                TreeRangeSet.create(ImmutableList.of(Range.closed(Long.MIN_VALUE, Long.MAX_VALUE))),
                LocalMasterableEntityDescriptor.of(type, "Open-End Mutual Fund"),
                TreeRangeSet.create(ImmutableList.of(Range.closed(Long.MIN_VALUE, Long.MAX_VALUE))),
                LocalMasterableEntityDescriptor.of(cusip, "26188J206"),
                TreeRangeSet.create(ImmutableList.of(Range.closed(Long.MIN_VALUE, Long.MAX_VALUE))),
                LocalMasterableEntityDescriptor.of(name, "Dreyfus Cash Management Institutional"),
                TreeRangeSet.create(
                    ImmutableList.of(Range.closed(Long.MIN_VALUE, Long.MAX_VALUE)))));

    personUniverse.append(e1);

    // create entity for second perspective
    final LocalMasterableEntityIdentity id2 = getRandomIdentity(issueEntityType, ciq, uid);

    final LocalMasterableEntity e2 =
        LocalMasterableEntity.of(
            issueEntityType,
            id2,
            ImmutableMap.of(
                LocalMasterableEntityDescriptor.of(ciq_security_id, "58441550"),
                TreeRangeSet.create(ImmutableList.of(Range.closed(Long.MIN_VALUE, Long.MAX_VALUE))),
                LocalMasterableEntityDescriptor.of(cusip, "26188J206"),
                TreeRangeSet.create(ImmutableList.of(Range.closed(Long.MIN_VALUE, Long.MAX_VALUE))),
                LocalMasterableEntityDescriptor.of(name, "DREYFUS CASH MANAGEMENT INSTL"),
                TreeRangeSet.create(
                    ImmutableList.of(Range.closed(Long.MIN_VALUE, Long.MAX_VALUE)))));

    personUniverse.append(e2);

    // resolve query
    final Map<LocalMasterEntity, Set<LocalMasterEntityMembership>> q1 =
        personUniverse.resolve(ResolveQuery.of(ImmutableMap.of(cusip, "26188J206")));
    Assert.assertEquals(q1.keySet().iterator().next().getDescriptors().size(), 5);

    final LocalMasterEntity ke1 = q1.keySet().iterator().next();

    Assert.assertTrue(ke1.getMemberIdentities().keySet().containsAll(ImmutableSet.of(id1, id2)));
  }

  private final RangeSet<Long> defaultRangeSet() {
    return TreeRangeSet.create(ImmutableSet.of(Range.closed(0L, Long.MAX_VALUE)));
  }

  @Test
  public void testFinancialMemberOfMultiKnowledgeEntityOverTime() {
    // create attributes for entity model
    final LocalEntityModel.Attribute axioma_id =
        LocalEntityModel.Attribute.of(
            "axioma_id", LocalEntityModel.AttributeType.STRING, EQUALITY_MATCHER);
    final LocalEntityModel.Attribute qaid =
        LocalEntityModel.Attribute.of(
            "qaid", LocalEntityModel.AttributeType.STRING, EQUALITY_MATCHER);
    final LocalEntityModel.Attribute sedol =
        LocalEntityModel.Attribute.of(
            "sedol", LocalEntityModel.AttributeType.STRING, EQUALITY_MATCHER);
    final LocalEntityModel.Attribute uid =
        LocalEntityModel.Attribute.of(
            "uid", LocalEntityModel.AttributeType.STRING, EQUALITY_MATCHER);

    // create entity model
    final LocalEntityModel equityEntityModel =
        LocalEntityModel.of(
            ImmutableSet.of(axioma_id, qaid, sedol, uid),
            ImmutableSet.of(
                LocalEntityModel.Resolver.of("qaid", ImmutableSet.of(qaid), 10),
                LocalEntityModel.Resolver.of("axioma_id", ImmutableSet.of(axioma_id), 9),
                LocalEntityModel.Resolver.of("sedol", ImmutableSet.of(sedol), 8)));

    // define entity type
    final EntityType equityEntityType = EntityType.of("equity_instrument", equityEntityModel);

    // define perspectives
    final String axioma = "axioma";
    final String refinitiv = "refinitiv";

    // create entity universe and add entity
    final LocalMemoryEntityUniverse universe = LocalMemoryEntityUniverse.of(equityEntityType);

    // create entity for first perspective
    final LocalMasterableEntityIdentity id1 = getRandomIdentity(equityEntityType, axioma, uid);

    final LocalMasterableEntity e1 =
        LocalMasterableEntity.of(
            equityEntityType,
            id1,
            ImmutableMap.of(
                LocalMasterableEntityDescriptor.of(axioma_id, "JMJ38PXZ0"),
                TreeRangeSet.create(
                    ImmutableList.of(
                        Range.closed(1L, Long.MAX_VALUE))), // Date.valueOf("2017-1-2"),
                // Date.valueOf("2079-12-31")
                LocalMasterableEntityDescriptor.of(sedol, "6056074"),
                TreeRangeSet.create(
                    ImmutableList.of(
                        Range.closed(
                            1L, 2L))), // Date.valueOf("2017-1-2"), Date.valueOf("2018-4-30")
                LocalMasterableEntityDescriptor.of(sedol, "BFXZDY1"),
                TreeRangeSet.create(
                    ImmutableList.of(
                        Range.closed(3L, Long.MAX_VALUE))))); // Date.valueOf("2018-5-1"),
    // Date.valueOf("2079-12-31")

    final Map<LocalMasterEntity, Set<LocalMasterEntityMembership>> d1 =
        universe.append(e1).getMapping();
    Assert.assertEquals(1, d1.keySet().iterator().next().getMemberIdentities().size());

    // create entity for second perspective
    final LocalMasterableEntityIdentity id2 = getRandomIdentity(equityEntityType, refinitiv, uid);

    final LocalMasterableEntity e2 =
        LocalMasterableEntity.of(
            equityEntityType,
            id2,
            ImmutableMap.of(
                LocalMasterableEntityDescriptor.of(qaid, "@ADVAN55-TW"),
                TreeRangeSet.create(
                    ImmutableList.of(
                        Range.closed(0L, Long.MAX_VALUE))), // Date.valueOf("1989-8-19"),
                // Date.valueOf("2079-12-31")
                LocalMasterableEntityDescriptor.of(sedol, "6056074"),
                TreeRangeSet.create(
                    ImmutableList.of(
                        Range.closed(3L, Long.MAX_VALUE))))); // Date.valueOf("2000-10-2"),
    // Date.valueOf("2079-12-31")

    final Map<LocalMasterEntity, Set<LocalMasterEntityMembership>> d2 =
        universe.append(e2).getMapping();
    Assert.assertEquals(1, d2.keySet().iterator().next().getMemberIdentities().size());

    final Map<LocalMasterEntity, Set<LocalMasterEntityMembership>> r1 =
        universe.resolve(ResolveQuery.of(ImmutableMap.of(sedol, "6056074")));

    Assert.assertNotNull(r1);
    Assert.assertEquals(2, r1.keySet().size());

    final LocalMasterableEntity e3 =
        LocalMasterableEntity.of(
            equityEntityType,
            id2,
            ImmutableMap.of(
                LocalMasterableEntityDescriptor.of(sedol, "6056074"),
                TreeRangeSet.create(
                    ImmutableList.of(
                        Range.closed(1L, Long.MAX_VALUE))))); // Date.valueOf("2000-10-2"),
    // Date.valueOf("2079-12-31")

    final LocalMemoryEntityUniverse.AppendResult d3 = universe.append(e3);

    Assert.assertEquals(1, d3.getMapping().size());
    Assert.assertEquals(2, d3.getMapping().keySet().iterator().next().getMemberIdentities().size());
  }

  @Test
  public void testCanMasterIncrementallyOverTime() {
    // create attributes
    final LocalEntityModel.Attribute uid =
        LocalEntityModel.Attribute.of(
            "uid", LocalEntityModel.AttributeType.STRING, EQUALITY_MATCHER);

    final LocalEntityModel.Attribute name =
        LocalEntityModel.Attribute.of(
            "name", LocalEntityModel.AttributeType.STRING, EQUALITY_MATCHER);

    final LocalEntityModel.Attribute email =
        LocalEntityModel.Attribute.of(
            "email", LocalEntityModel.AttributeType.STRING, EQUALITY_MATCHER);

    final LocalEntityModel.Attribute username =
        LocalEntityModel.Attribute.of(
            "username", LocalEntityModel.AttributeType.STRING, EQUALITY_MATCHER);

    final LocalEntityModel.Attribute publicKey =
        LocalEntityModel.Attribute.of(
            "publicKey", LocalEntityModel.AttributeType.STRING, EQUALITY_MATCHER);

    // create entity model
    final LocalEntityModel personEntityModel =
        LocalEntityModel.of(
            ImmutableSet.of(uid, name, email, username, publicKey),
            ImmutableSet.of(
                LocalEntityModel.Resolver.of("uid", ImmutableSet.of(uid), 10),
                LocalEntityModel.Resolver.of("publicKey", ImmutableSet.of(publicKey), 9),
                LocalEntityModel.Resolver.of("email_username", ImmutableSet.of(username, email), 8),
                LocalEntityModel.Resolver.of("name_username", ImmutableSet.of(name, username), 7),
                LocalEntityModel.Resolver.of("name", ImmutableSet.of(name), 6),
                LocalEntityModel.Resolver.of("email", ImmutableSet.of(email), 5),
                LocalEntityModel.Resolver.of("username", ImmutableSet.of(email), 4)));

    // define entity type
    final EntityType personEntityType = EntityType.of("person", personEntityModel);

    // create entity universe and add entity
    final LocalMemoryEntityUniverse universe = LocalMemoryEntityUniverse.of(personEntityType);

    // create a maximum of 2000 entities
    final int MAX_ENTITIES = 2000;

    final Set<LocalMasterableEntity> entities = Sets.newHashSet();

    // create a random masterable entity and assign a uuid as its identity
    for (int i = 0; i < MAX_ENTITIES; ++i) {
      entities.add(getRandomMasterableEntity(personEntityType, RandomStringUtils.randomAlphanumeric(10), uid));
    }

    // add all masterable entities to the same mastering universe
    for (LocalMasterableEntity masterableEntity : entities) {
      universe.append(masterableEntity);
    }

    // assert all random entities exist in the mastering universe
    assert universe.getMasterEntities().size() == entities.size();

    // add the same name for all entities, but at non overlapping periods
    long currentRange = 0;

    final String sampleName = RandomStringUtils.randomAlphabetic(30);

    for (LocalMasterableEntity masterableEntity : entities) {
      universe.append(
          LocalMasterableEntity.of(
              masterableEntity.getType(),
              masterableEntity.getMasterableEntityIdentity(),
              ImmutableMap.of(
                  LocalMasterableEntityDescriptor.of(name, sampleName),
                  TreeRangeSet.create(
                      ImmutableSet.of(Range.open(currentRange, currentRange + 1))))));

      currentRange += 1;
    }

    // assert that all masterable entities exist in the mastering universe
    assert universe.getMasterEntities().size() == entities.size();

    final String sampleEmail = RandomStringUtils.randomAlphabetic(20);

    // split half the entities for next collapsing testing
    final Set<LocalMasterableEntity> collapsed =
        entities.parallelStream().skip(entities.size() / 2).collect(Collectors.toSet());

    // take the other half of uncollapsed entities and hold onto those for testing
    final Set<LocalMasterableEntity> uncollapsed =
        Sets.difference(entities, collapsed);

    // assert the collapsed + uncoollapsed entities are equal to the total number of entities
    assert collapsed.size() + uncollapsed.size() == entities.size();

    // collapse half of the entities into one by adding a new characteristic with overlapping temporality
    collapsed.parallelStream().forEach(masterableEntity -> {
      LocalMemoryEntityUniverse.AppendResult ar =  universe.append(
          LocalMasterableEntity.of(
              masterableEntity.getType(),
              masterableEntity.getMasterableEntityIdentity(),
              ImmutableMap.of(
                  LocalMasterableEntityDescriptor.of(email, sampleEmail),
                  TreeRangeSet.create(ImmutableSet.of(Range.open(0L, Long.MAX_VALUE))))));

      assert ar.getDeprecations().size() == 1;
    });

    // assert we have half of the entities plus one (the mastered entity)
    assert universe.getMasterEntities().size() == (entities.size() / 2) + 1;

    final Map<LocalMasterEntity, Set<LocalMasterEntityMembership>> r1 =
        universe.resolve(ResolveQuery.of(ImmutableMap.of(email, sampleEmail)));

    // assert we have only one master entity matching the email characteristic
    assert r1.keySet().size() == 1;
    assert r1.values().parallelStream().allMatch(x -> x.size() == 1);
    assert r1.keySet().iterator().next().getMemberIdentities().size() ==  (entities.size() / 2);

    // collapse the uncollapsed by adding the sample email characteristic to the rest of the entities in the universe
    uncollapsed.parallelStream().forEach(masterableEntity -> {
      LocalMemoryEntityUniverse.AppendResult ar = universe.append(
          LocalMasterableEntity.of(
              masterableEntity.getType(),
              masterableEntity.getMasterableEntityIdentity(),
              ImmutableMap.of(
                  LocalMasterableEntityDescriptor.of(email, sampleEmail),
                  TreeRangeSet.create(ImmutableSet.of(Range.open(0L, Long.MAX_VALUE))))));

      assert ar.getDeprecations().size() == 1;
    });

    // assert now that we have only one master entity
    assert universe.getMasterEntities().size() == 1;

    final LocalMasterableEntity m =
        getRandomMasterableEntity(personEntityType, RandomStringUtils.randomAlphanumeric(10), uid);

    final String sampleUsername = RandomStringUtils.randomAlphabetic(30);

    // add new masterable entity with overlapping characteristics
    LocalMemoryEntityUniverse.AppendResult ar = universe.append(
        LocalMasterableEntity.of(
            m.getType(),
            m.getMasterableEntityIdentity(),
            ImmutableMap.of(
                LocalMasterableEntityDescriptor.of(email, sampleEmail),
                TreeRangeSet.create(ImmutableSet.of(Range.open(0L, Long.MAX_VALUE))),
                LocalMasterableEntityDescriptor.of(username, sampleUsername),
                TreeRangeSet.create(ImmutableSet.of(Range.open(0L, Long.MAX_VALUE)))
            )));

    // assert deprecations have not increased
    assert ar.getDeprecations().size() == 1;
    assert ar.getMapping().size() == 1;
    assert ar.getMapping().keySet().iterator().next().getMemberIdentities().size() == entities.size() + 1;
    assert universe.getMasterEntities().size() == 1;
  }
}
