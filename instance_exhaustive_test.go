package redbuckets

import (
	"fmt"
	"testing"
)

func TestInstance_targetBuckets_Exhaustive(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skipping exhaustive test in short mode")
	}

	for bucketsTotal := 1; bucketsTotal <= 100; bucketsTotal++ {
		if bucketsTotal%2 != 0 {
			continue
		}
		t.Run(fmt.Sprintf("Buckets_%d", bucketsTotal), func(t *testing.T) {
			t.Parallel()
			for instancesCount := 1; instancesCount <= 102; instancesCount++ {
				assignedBuckets := make(map[uint16]int)
				totalAssigned := 0

				for instanceIndex := 0; instanceIndex < instancesCount; instanceIndex++ {
					i := &Instance{
						bucketsTotalCount: bucketsTotal,
					}
					buckets := i.targetBuckets(instanceIndex, instancesCount)

					for _, b := range buckets {
						assignedBuckets[b]++
						totalAssigned++
					}

					// Check that within one instance there are no duplicates
					// (Though the logic is simple enough that this is unlikely)
					instanceMap := make(map[uint16]bool)
					for _, b := range buckets {
						if instanceMap[b] {
							t.Errorf("Duplicate bucket %d in instance %d (total buckets: %d, instances: %d)", b, instanceIndex, bucketsTotal, instancesCount)
						}
						instanceMap[b] = true
					}
				}

				// 1. Check total count
				if totalAssigned != bucketsTotal {
					t.Errorf("Total assigned buckets %d != expected %d (instances: %d)", totalAssigned, bucketsTotal, instancesCount)
				}

				// 2. Check coverage and no overlaps
				for b := 0; b < bucketsTotal; b++ {
					count, ok := assignedBuckets[uint16(b)]
					if !ok {
						t.Errorf("Bucket %d not assigned (total buckets: %d, instances: %d)", b, bucketsTotal, instancesCount)
					} else if count > 1 {
						t.Errorf("Bucket %d assigned %d times (total buckets: %d, instances: %d)", b, count, bucketsTotal, instancesCount)
					}
				}

				// 3. Check if any bucket outside range was assigned
				if len(assignedBuckets) != bucketsTotal {
					t.Errorf("Number of unique assigned buckets %d != expected %d (instances: %d)", len(assignedBuckets), bucketsTotal, instancesCount)
				}
			}
		})
	}
}
