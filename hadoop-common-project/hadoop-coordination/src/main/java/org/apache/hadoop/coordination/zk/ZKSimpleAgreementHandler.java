package org.apache.hadoop.coordination.zk;

import java.io.IOException;

import org.apache.hadoop.coordination.Agreement;

/**
 * @author Andrey Stepachev
 */
public class ZKSimpleAgreementHandler<L, I extends Agreement<L, ?>> implements ZKAgreementHandler<L, I> {
  private Class<I> agreementClz;
  private L learner;


  public ZKSimpleAgreementHandler(Class<I> agreementClz, L learner) {
    this.agreementClz = agreementClz;
    this.learner = learner;
  }

  @Override
  public L getLearner() {
    return learner;
  }

  @Override
  public void setLearner(L learner) {
    this.learner = learner;
  }

  @Override
  public boolean handles(Agreement<L, ?> agreement) {
    return agreement.getClass().isAssignableFrom(agreementClz);
  }

  @Override
  public void executeAgreement(Agreement<L, ?> agreement) throws IOException {
    agreement.execute(learner);
  }
}
