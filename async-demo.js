
/*
 * FILE: async-demo.js
 * PURPOSE: Demonstrate synchronous and asynchronous execution.
 */

// STEP 1: Run immediately.
console.log('1. Server starting');

// STEP 2: Schedule a callback for approximately one second later.
setTimeout(() => {
  console.log('3. One second has passed');
}, 1000);

// STEP 3: Continue executing immediately.
console.log('2. Server setup complete');
