const { z } = require('zod');

const employeeSchema = z.object({
  empCode: z.string().optional(),
  geoX: z.number().optional(),
  geoY: z.number().optional(),
  gender: z.string().optional(),
  isMedical: z.boolean().optional()
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

const profileSchema = z.object({
  id: z.number().optional(),
  name: z.string().optional(),
  zoneClubbing: z.boolean({ required_error: "zoneClubbing is required and must be a boolean" }),
  zoneBasedRouting: z.boolean({ required_error: "zoneBasedRouting is required and must be a boolean" }),
  LargeCapacityZones: z.array(z.string()).optional(),
  MediumCapacityZones: z.array(z.string()).optional(),
  SmallCapacityZones: z.array(z.string()).optional(),
  zonePairingMatrix: z.record(z.string(), z.array(z.string())).optional(),
  isAutoClubbing: z.boolean().optional(),
  maxDuration: z.number({ required_error: "maxDuration is required and must be a number" }),
  // ...other fields
}).passthrough();

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
  tripType: z.string({ required_error: "tripType is required" })
    .refine(
      v => ['P', 'D', 'PICKUP', 'DROPOFF'].includes(v.toUpperCase()),
      { message: 'tripType must be "P", "D", "PICKUP", or "DROPOFF"' }
    ),
  guard: z.boolean({ required_error: "guard is required and must be a boolean" }),
  zones: z.array(z.any()).optional(),
  saveToDatabase: z.boolean().optional()
});

module.exports = { routeRequestSchema };
