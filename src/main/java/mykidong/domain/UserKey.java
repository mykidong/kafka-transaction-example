package mykidong.domain;

import java.util.Date;
import java.util.Objects;

/**
 * Created by mykidong on 2019-09-11.
 */
public class UserKey {

    private String customerId;
    private Date transactionDate;

    public UserKey(String customerId, Date transactionDate) {
        this.customerId = customerId;
        this.transactionDate = transactionDate;
    }

    public String getCustomerId() {
        return customerId;
    }

    public Date getTransactionDate() {
        return transactionDate;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof UserKey)) return false;
        UserKey that = (UserKey) o;
        return Objects.equals(customerId, that.customerId) &&
                Objects.equals(transactionDate, that.transactionDate);
    }

    @Override
    public int hashCode() {
        return Objects.hash(customerId, transactionDate);
    }
}
