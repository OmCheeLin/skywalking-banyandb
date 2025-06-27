// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package encoding

import (
	"fmt"
	"math"
)

// Float64ListToDecimalIntList converts float64 values to int64s with a common decimal scale factor.
func Float64ListToDecimalIntList(nums []float64) ([]int64, int32, error) {
	if len(nums) == 0 {
		return nil, 0, fmt.Errorf("nums must contain at least one item")
	}

	// Maximum allowed decimal places for float64 safety
	const maxDecimalPlaces = 15
	var maxPlaces int32 = 0

	// Determine the maximum number of decimal places needed
	for _, f := range nums {
		if math.IsNaN(f) || math.IsInf(f, 0) {
			return nil, 0, fmt.Errorf("invalid float value: %v", f)
		}
		places := countDecimalPlaces(f, maxDecimalPlaces)
		if places > maxPlaces {
			maxPlaces = places
		}
	}

	scale := math.Pow10(int(maxPlaces))
	values := make([]int64, len(nums))
	for i, f := range nums {
		scaled := f * scale
		rounded := math.Round(scaled)
		if rounded > math.MaxInt64 || rounded < math.MinInt64 {
			// // -1 and err represents an overflow condition
			return nil, -1, fmt.Errorf("value %f overflows int64", rounded)
		}
		values[i] = int64(rounded)
	}

	return values, -maxPlaces, nil
}

// DecimalIntListToFloat64List restores float64 values from scaled int64s using a decimal exponent.
func DecimalIntListToFloat64List(values []int64, exponent int32) ([]float64, error) {
	if len(values) == 0 {
		return nil, nil
	}
	scale := math.Pow10(int(-exponent))
	floats := make([]float64, len(values))
	for i, v := range values {
		floats[i] = float64(v) / scale
	}
	return floats, nil
}

// countDecimalPlaces estimates the number of decimal places in a float64 up to a given maximum.
func countDecimalPlaces(f float64, max int) int32 {
	for i := 0; i < max; i++ {
		f *= 10
		if math.Abs(f-math.Round(f)) < 1e-9 {
			return int32(i + 1)
		}
	}
	return int32(max)
}
