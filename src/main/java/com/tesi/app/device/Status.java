package com.tesi.app.device;

import java.sql.Timestamp;

import org.eclipse.ditto.base.model.common.HttpStatus;
import org.eclipse.ditto.client.DittoClient;
import org.eclipse.ditto.json.JsonObject;
import org.eclipse.ditto.json.JsonPointer;
import org.eclipse.ditto.protocol.Adaptable;
import org.eclipse.ditto.protocol.Payload;
import org.eclipse.ditto.protocol.TopicPath;
import org.eclipse.ditto.things.model.ThingId;

public class Status implements Runnable {
	
	public DittoClient client;
	public String status = "start";
	public String id;
	
	Status(DittoClient clientAtDevice, String id){
		this.client = clientAtDevice;
		this.id = id;
	}

	@Override
	public void run() {
		// TODO Auto-generated method stub
		while(true) {
			try {
				updateStatus();
				Thread.sleep(10000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

	}
	
	private void updateStatus() {
		final Adaptable modifyStatusProperties = setStatusProperty(this.id, this.status);
		
		this.client.sendDittoProtocol(modifyStatusProperties)
		.whenComplete(((adaptable, throwable) -> {
			if (throwable != null) {
				System.out.println("errore nell'aggiornamento dello stato");
			} else {
				System.out.println("status aggiornato correttamente a " + status);
				App.getDigitalThing(this.client, this.id);
			}
		}));
	}
	
	private static Adaptable setStatusProperty(String thingId, String status) {
		
	    Timestamp timestamp = new Timestamp(System.currentTimeMillis());
	    System.out.println(timestamp.getTime());
	    
		Adaptable modifyFrequencyProperties = Adaptable
				.newBuilder(TopicPath.newBuilder(ThingId.of(thingId))
					.things()
					.twin()
					.commands()
					.modify()
					.build())
				.withPayload(Payload
						.newBuilder(JsonPointer.of("/attributes/status"))
						.withValue(JsonObject.newBuilder()
								.set("status", status)
								.set("lastUpdate", timestamp.getTime()).build())
						.build())
				.build();
		return modifyFrequencyProperties;
	}

}
