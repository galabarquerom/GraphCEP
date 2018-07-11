package com.cor.cep;

import java.io.IOException;

import com.cor.cep.controller.MotorbikeController;

public class Main {

	/**
	 * Main method
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException{

		MotorbikeController controller = new MotorbikeController();
		controller.start();
	}

}
