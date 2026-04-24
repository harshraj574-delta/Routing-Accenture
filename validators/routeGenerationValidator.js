const { z } = require('zod');

const TRIP_TYPE_ALIASES = {
  P: "PICKUP",
  PICKUP: "PICKUP",
  D: "DROPOFF",
  DROP: "DROPOFF",
  DROPOFF: "DROPOFF",
};

const tripTypeSchema = z.string({ required_error: "tripType is required" })
  .transform(value => value.trim().toUpperCase())
  .refine(
    value => Object.prototype.hasOwnProperty.call(TRIP_TYPE_ALIASES, value),
    { message: 'tripType must be "P", "D", "PICKUP", "DROP", or "DROPOFF"' }
  )
  .transform(value => TRIP_TYPE_ALIASES[value]);

const employeeSchema = z.object({
  empCode: z.string().optional(),
  geoX: z.number().optional(),
  geoY: z.number().optional(),
  gender: z.string().optional(),
  isMedical: z.boolean().optional(),
  isPWD: z.boolean().optional().default(false),
  isNMT: z.boolean().optional().default(false),
  isOOB: z.boolean().optional().default(false),
  isNewlyAdded: z.boolean().optional().default(false)  // Flag for insertion-based reoptimization
});

const employeesArraySchema = z.array(employeeSchema).min(1, { message: "At least one employee is required" }).superRefine((employees, ctx) => {
  employees.forEach((emp, idx) => {
    if (emp.geoX == null || isNaN(emp.geoX)) {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        message: `geoX is required for empCode: ${emp.empCode ?? '[unknown]'}`,
        path: [idx, 'geoX'],
      });
    } else if (emp.geoX < 68 || emp.geoX > 98) {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        message: `geoX (longitude) for empCode: ${emp.empCode ?? '[unknown]'} must be within India (68-98)`,
        path: [idx, 'geoX'],
      });
    }
    if (emp.geoY == null || isNaN(emp.geoY)) {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        message: `geoY is required for empCode: ${emp.empCode ?? '[unknown]'}`,
        path: [idx, 'geoY'],
      });
    } else if (emp.geoY < 6 || emp.geoY > 38) {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        message: `geoY (latitude) for empCode: ${emp.empCode ?? '[unknown]'} must be within India (6-38)`,
        path: [idx, 'geoY'],
      });
    }
    if (!emp.empCode) {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        message: `empCode is required for one of the employees`,
        path: [idx, 'empCode'],
      });
    }
    if (!emp.gender || !/^[MF]$/.test(emp.gender)) {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        message: `gender must be 'M' or 'F' for empCode: ${emp.empCode ?? '[unknown]'}`,
        path: [idx, 'gender'],
      });
    }
  });
});

const facilitySchema = z.object({
  geoX: z.number({ required_error: "Facility geoX is required" })
    .min(68, { message: "Facility geoX (longitude) must be within India (68-98)" })
    .max(98, { message: "Facility geoX (longitude) must be within India (68-98)" }),
  geoY: z.number({ required_error: "Facility geoY is required" })
    .min(6, { message: "Facility geoY (latitude) must be within India (6-38)" })
    .max(38, { message: "Facility geoY (latitude) must be within India (6-38)" }),
  // Add other fields as needed
});

// --- Schema for Night Shift Timings ---
const nightShiftTimingsSchema = z.object({
  start: z.number().int().min(0).max(2359, "Start time must be 0000-2359 (numeric, e.g., 2000 for 8 PM)"),
  end: z.number().int().min(0).max(2359, "End time must be 0000-2359 (numeric, e.g., 700 for 7 AM)"),
});

const routeDeviationRuleSchema = z.object({
  minDistKm: z.number().nonnegative(),
  maxDistKm: z.number().positive(),
  // thresholdPercent: z.number().min(0).max(100), // Optional, if you use it
  // exampleDistanceKm: z.number().positive(),    // Optional, if you use it
  // acceptableDeviationKm: z.number().nonnegative(), // Optional, if you use it
  maxTotalOneWayKm: z.number().positive({ message: "maxTotalOneWayKm is required and must be positive" }),
});

const fleetVehicleSchema = z.object({
  type: z.string(), // e.g., "4-seater", "6/7-seater", "12-seater"
  capacity: z.number().int().positive(), // e.g., 3, 5, 8
  count: z.number().int().nonnegative()  // e.g., 82, 378, 105
});

const profileSchema = z.object({
  id: z.number().optional(),
  name: z.string().optional(),
  city: z.string().optional(),
  zoneClubbing: z.boolean({ required_error: "zoneClubbing is required and must be a boolean" }),
  zoneBasedRouting: z.boolean({ required_error: "zoneBasedRouting is required and must be a boolean" }),
  // SL #5: Night Shift Guard Timings - THIS IS THE KEY ADDITION FOR CURRENT GUARD LOGIC
  nightShiftGuardTimings: z.record(z.string(), nightShiftTimingsSchema)
    .optional() // Making it optional so your code can use defaults if not provided
    .refine(val => { // Optional: Validate keys if you want to be strict
      if (!val) return true; // Allow it to be undefined
      const allowedKeys = ["PICKUP", "DROPOFF", "CDC_PICKUP", "CDC_DROPOFF", "DDC_PICKUP", "DDC_DROPOFF"];
      return Object.keys(val).every(key => allowedKeys.includes(key.toUpperCase()));
    }, { message: "Invalid keys in nightShiftGuardTimings. Allowed keys are like PICKUP, CDC_DROPOFF, etc." }),
  facilityType: z.enum(["CDC", "DDC"]).optional(), // To select correct rule set
  routeDeviationRules: z.record(z.string(), z.array(routeDeviationRuleSchema).min(1)) // e.g., "CDC": [rules]
    .optional(),
  LargeCapacityZones: z.array(z.string()).optional(),
  MediumCapacityZones: z.array(z.string()).optional(),
  SmallCapacityZones: z.array(z.string()).optional(),
  zonePairingMatrix: z.record(z.string(), z.array(z.string())).optional(),
  isAutoClubbing: z.boolean().optional(),
  maxDuration: z.number({ required_error: "maxDuration is required and must be a number" }),
  fleet: z.array(fleetVehicleSchema).optional(),
  // Route Consolidation Configuration
  enableRouteConsolidation: z.boolean().optional().default(true),
  consolidationMaxInsertionDistanceKm: z.number().positive().optional().default(5),
  consolidationLowOccupancyThreshold: z.number().int().positive().optional().default(2),
  consolidationMinHostOccupancy: z.number().int().positive().optional().default(1),
  consolidationLinearTolerance: z.number().min(0).max(180).optional().default(90),
  enableBidirectionalConsolidation: z.boolean().optional().default(true),
  // Heuristic Configuration
  heuristicMaxStopDistance: z.number().positive().optional().default(4.0),
  heuristicLinearTolerance: z.number().min(0).max(180).optional().default(75),
  heuristicDirectionPenaltyWeight: z.number().optional().default(5.0),
  heuristicCandidatePoolSize: z.number().int().positive().optional().default(6),
  heuristicOsrmEvaluationTopK: z.number().int().positive().optional().default(3),
  heuristicBearingPenaltyWeight: z.number().nonnegative().optional().default(1.5),
  heuristicFacilityOrderPenaltyWeight: z.number().nonnegative().optional().default(4.0),
  heuristicDistanceSpreadPenaltyWeight: z.number().nonnegative().optional().default(0.8),
  heuristicDurationWeight: z.number().nonnegative().optional().default(0.0025),
  // Singleton Aggregation Configuration
  singletonAggregationRadius: z.number().positive().optional().default(6),
  singletonMaxClusterSize: z.number().int().positive().optional().default(4),
  // Cross-Route Optimization Configuration
  enableCrossRouteOptimization: z.boolean().optional().default(true),
  crossRouteMaxIterations: z.number().int().positive().optional().default(4),
  crossRouteMaxDetourKm: z.number().positive().optional().default(4.5),
  crossRouteMaxDetourRatio: z.number().positive().optional().default(1.2),
  crossRouteMaxHeadingDeviationDeg: z.number().min(0).max(180).optional().default(100),
  crossRouteMonotonicitySlackKm: z.number().nonnegative().optional().default(1.8),
  crossRouteCorridorRadiusKm: z.number().positive().optional().default(5.0),
  crossRouteTopHostCandidates: z.number().int().positive().optional().default(5),
  crossRouteMaxOsrmChecksPerIteration: z.number().int().positive().optional().default(250),
  crossRouteRouteReductionBonusKm: z.number().nonnegative().optional().default(2.0),
  crossRouteOccupancyBonusKm: z.number().nonnegative().optional().default(0.8),
  crossRoutePassengerBurdenWeight: z.number().nonnegative().optional().default(0.35),
  crossRouteMinNetGainKm: z.number().optional().default(-0.2),
  crossRouteCandidateEmployeesPerRoute: z.number().int().positive().optional().default(6),
  // Local Sequence Optimization (within each route)
  enableLocalSequenceOptimization: z.boolean().optional().default(true),
  localSequenceMaxIterations: z.number().int().positive().optional().default(3),
  localSequenceDirectionPenaltyWeight: z.number().nonnegative().optional().default(4.0),
  localSequencePassengerBurdenWeight: z.number().nonnegative().optional().default(0.25),
  localSequenceMonotonicSlackKm: z.number().nonnegative().optional().default(1.5),
  // ...other fields
}).strict();

const routeRequestSchema = z.object({
  employees: employeesArraySchema,
  facility: facilitySchema,
  shiftTime: z.string({ required_error: "shiftTime is required" })
    .regex(/^\d{4}$/, { message: "shiftTime must be in HHMM (0000-2359) format" }),
  date: z.string({ required_error: "date is required" }),
  profile: profileSchema,
  pickupTimePerEmployee: z.number({ required_error: "pickupTimePerEmployee is required" })
    .positive({ message: "pickupTimePerEmployee must be a positive number (in seconds)" }),
  reportingTime: z.number({ required_error: "reportingTime is required" })
    .min(0, { message: "reportingTime must be a positive number (in seconds)" }),
  tripType: tripTypeSchema,
  guard: z.boolean({ required_error: "guard is required and must be a boolean" }),
  zones: z.array(z.any()).optional(),
  saveToDatabase: z.boolean().optional()
});

const recalculateRouteRequestSchema = z.object({
  routes: z.array(z.object({
    routeId: z.union([z.string(), z.number()]),
    employees: employeesArraySchema,
  })),
  facility: facilitySchema,
  shiftTime: z.string().regex(/^\d{4}$/, { message: "shiftTime must be in HHMM format" }),
  pickupTimePerEmployee: z.number().positive(),
  reportingTime: z.coerce.number().min(0),
  city: z.string(),
  tripType: tripTypeSchema,
});

const geocodesSchema = z.object({
  lat: z.number().min(-90).max(90),
  lng: z.number().min(-180).max(180)
});

const etaRequestSchema = z.object({
  routes: z.array(z.object({
    routeId: z.union([z.string(), z.number()]),
    currentGeocodes: geocodesSchema,
    destinationGeocodes: geocodesSchema
  })).min(1),
  shiftTime: z.string().regex(/^\d{4}$/, { message: "shiftTime must be in HHMM format" }),
  city: z.string()
});

module.exports = { routeRequestSchema, recalculateRouteRequestSchema, etaRequestSchema };
