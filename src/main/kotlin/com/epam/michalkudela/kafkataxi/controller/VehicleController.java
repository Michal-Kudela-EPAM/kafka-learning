package com.epam.michalkudela.kafkataxi.controller;

import com.epam.michalkudela.kafkataxi.model.VehiclePosition;
import com.epam.michalkudela.kafkataxi.service.PositionService;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
@AllArgsConstructor
@Slf4j
public class VehicleController {
    private PositionService positionService;

    @PutMapping("/vehicle/position")
    public String acceptVehicleSignal(@RequestBody VehiclePosition vehiclePosition) {
        log.info("got request: '{}'", vehiclePosition);
        positionService.acceptVehicleSignal(vehiclePosition);
        return "ok";
    }
}
