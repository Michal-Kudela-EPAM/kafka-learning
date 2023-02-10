package com.epam.michalkudela.kafkataxi.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@NoArgsConstructor
@Data
@Builder
public class VehicleDistance {
    private String id;
    private Double distance;
    private Double xCoordinate;
    private Double yCoordinate;
}
