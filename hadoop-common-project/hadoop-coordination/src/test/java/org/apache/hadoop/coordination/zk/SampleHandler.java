package org.apache.hadoop.coordination.zk;

/**
 * @author Andrey Stepachev
 */
public class SampleHandler extends ZKSimpleAgreementHandler<SampleLearner, SampleProposal> {
  public SampleHandler(SampleLearner learner) {
    super(SampleProposal.class, learner);
  }
}
