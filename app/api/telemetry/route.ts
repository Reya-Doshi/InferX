// app/api/telemetry/route.ts
import { NextRequest } from "next/server";
import { engineState } from "@/lib/engineState";

export const dynamic = "force-dynamic";

export async function GET(req: NextRequest) {
  const encoder = new TextEncoder();

  const stream = new ReadableStream({
    start(controller) {
      // Send initial metrics snapshot immediately
      const initialMetrics = engineState.getMetrics();
      controller.enqueue(encoder.encode(`data: ${JSON.stringify(initialMetrics)}\n\n`));

      // Push telemetry every 1000ms
      const intervalId = setInterval(() => {
        try {
          const metrics = engineState.getMetrics();
          controller.enqueue(encoder.encode(`data: ${JSON.stringify(metrics)}\n\n`));
        } catch {
          clearInterval(intervalId);
          controller.close();
        }
      }, 1000);

      // Handle client disconnect / abort cleanly
      req.signal.onabort = () => {
        clearInterval(intervalId);
        try {
          controller.close();
        } catch {
          // Controller might already be closed
        }
      };
    },
  });

  return new Response(stream, {
    headers: {
      "Content-Type": "text/event-stream",
      "Cache-Control": "no-cache, no-transform",
      Connection: "keep-alive",
      "Access-Control-Allow-Origin": "*",
    },
  });
}
