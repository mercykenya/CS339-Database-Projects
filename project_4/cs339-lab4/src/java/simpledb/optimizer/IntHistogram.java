package simpledb.optimizer;

import simpledb.execution.Predicate;

/**
 * A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    private int nBuckets;         // Number of buckets to split the input value into
    private int width;      // Size of each bucket
    private int min;              // Minimum integer value for histogramming
    private int max;              // Maximum integer value for histogramming
    private int[] hist;           // Array to store the count of values in each bucket
    private int nTups;            // Total number of values added to the histogram

    /**
     * Create a new IntHistogram.
     * <p>
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * <p>
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * <p>
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't
     * simply store every value that you see in a sorted list.
     *
     * @param buckets The number of buckets to split the input value into.
     * @param min     The minimum integer value that will ever be passed to this class for histogramming
     * @param max     The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
        // DONE

        this.nBuckets = buckets;
        this.min = min;
        this.max = max;
        this.hist = new int[buckets];
        this.width = (int) Math.ceil((double) (max - min + 1) / buckets);
        for (int i=0; i< hist.length;i++) {
            hist[i]= 0;
        }
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     *
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
        // DONE
        // Increment the count for the corresponding bucket based on the value 'v'

        if (v == min) {
        // If 'v' is equal to the minimum value, increment the count in the first bucket
        hist[0]++;
        } else if (v == max) {
        // If 'v' is equal to the maximum value, increment the count in the last bucket
        hist[nBuckets - 1]++;
        } else {
        // Calculate the index of the bucket for the given value 'v'
        int bucketIndex = (v - min) / width;
        // Increment the count in the corresponding bucket
        hist[bucketIndex]++;
        }

        // Increment the total number of values added
        nTups++;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * <p>
     * For example, if "op" is "GREATER_THAN" and "v" is 5,
     * return your estimate of the fraction of elements that are greater than 5.
     *
     * @param op Operator
     * @param v  Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {

        // DONE
        int bucketIndex = (v - min) / width;
        int height;
        int left = bucketIndex * width + min;
        int right = bucketIndex * width + min + width - 1;
    
        if (op == Predicate.Op.EQUALS) {
            if (v < min || v > max) {
                return 0.0;
            } else {
                height = hist[bucketIndex];
                return (double) (height / width) / nTups;
            }
        }
        if (op == Predicate.Op.GREATER_THAN) {
            if (v < min) {
                return 1.0;
            }
            if (v > max - 1) {
                return 0.0;
            } else {
                height = hist[bucketIndex];
                double b_f = (double) height / nTups;
                double b_part = (double) (right - v) / width;
                double answer = b_f * b_part;
    
                for (int i = bucketIndex + 1; i < hist.length; i++) {
                    int height2 = hist[i];
                    double b_f2 = (double) height2 / nTups;
                    answer += b_f2;
                }
                return answer;
            }
        }
        if (op == Predicate.Op.LESS_THAN) {
            if (v <= min) {
                return 0.0;
            }
            if (v > max) {
                return 1.0;
            } else {
                height = hist[bucketIndex];
                double b_f = (double) height / nTups;
                double b_part = (double) (v - left) / width;
                double answer = b_f * b_part;
    
                for (int i = bucketIndex - 1; i >= 0; i--) {
                    int height2 = hist[i];
                    double b_f2 = (double) height2 / nTups;
                    answer += b_f2;
                }
                return answer;
            }
        }
        if (op == Predicate.Op.LESS_THAN_OR_EQ) {
            if (v < min) {
                return 0.0;
            }
            if (v >= max) {
                return 1.0;
            } else {
                height = hist[bucketIndex];
                double b_f = (double) height / nTups;
                double b_part = (double) (v - left) / width;
                double answer = b_f * b_part;
    
                for (int i = bucketIndex - 1; i >= 0; i--) {
                    int height2 = hist[i];
                    double b_f2 = (double) height2 / nTups;
                    answer += b_f2;
                }
                answer += (double) (height / width) / nTups;
                return answer;
            }
        }
        if (op == Predicate.Op.GREATER_THAN_OR_EQ) {
            if (v <= min) {
                return 1.0;
            }
            if (v > max) {
                return 0.0;
            } else {
                height = hist[bucketIndex];
                double b_f = (double) height / nTups;
                double b_part = (double) (right - v) / width;
                double answer = b_f * b_part;
    
                for (int i = bucketIndex + 1; i < hist.length; i++) {
                    int height2 = hist[i];
                    double b_f2 = (double) height2 / nTups;
                    answer += b_f2;
                }
                answer += (double) (height / width) / nTups;
                return answer;
            }
        }
        if (op == Predicate.Op.LIKE) {
            if (v < min || v > max) {
                return 0.0;
            } else {
                height = hist[bucketIndex];
                return (double) (height / width) / nTups;
            }
        }
        if (op == Predicate.Op.NOT_EQUALS) {
            if (v < min || v > max) {
                return 1.0;
            } else {
                height = hist[bucketIndex];
                double answer = (double) (height / width) / nTups;
                return 1.0 - answer;
            }
        }
        return 0.0; // Should not reach this point unless something went wrong
        
        
    }

    /**
     * @return the average selectivity of this histogram.
     *         <p>
     *         This is not an indispensable method to implement the basic
     *         join optimization. It may be needed if you want to
     *         implement a more efficient optimization
     */
    public double avgSelectivity() {
        // DONE
        int totalCount = 0;
    
        // Calculate the total count of values in all buckets
        for (int count : hist) {
            totalCount += count;
        }
    
        // Calculate the average selectivity
        double avgSelectivity = (double) totalCount / nTups;
        
        return avgSelectivity;
    }

    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // DONE
        StringBuilder sb = new StringBuilder();
    
        sb.append("Histogram Information:")
            .append("\n- Number of Buckets: ").append(nBuckets)
            .append("\n- Minimum Value: ").append(min)
            .append("\n- Maximum Value: ").append(max)
            .append("\n- Bucket Size: ").append(width)
            .append("\n- Total Number of Values: ").append(nTups)
            .append("\n\nBucket Counts:");
        
        // Append the count of values in each bucket
        for (int i = 0; i < nBuckets; i++) {
            sb.append("\nBucket ").append(i).append(": ").append(hist[i]);
        }
        
        return sb.toString();
    }
}
