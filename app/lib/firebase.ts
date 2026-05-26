// Import the functions you need from the SDKs you need
import { initializeApp, getApps } from 'firebase/app';
import { getAnalytics, isSupported } from 'firebase/analytics';
// TODO: Add SDKs for Firebase products that you want to use
// https://firebase.google.com/docs/web/setup#available-libraries

// Your web app's Firebase configuration
// For Firebase JS SDK v7.20.0 and later, measurementId is optional
const firebaseConfig = {
  apiKey: "AIzaSyCQE9pp_lzjWMWhQyPdEMhBXfTMJ2pjPNs",
  authDomain: "rxjs-marbles.firebaseapp.com",
  projectId: "rxjs-marbles",
  // storageBucket: "rxjs-marbles.firebasestorage.app",
  // messagingSenderId: "226023109332",
  appId: "1:226023109332:web:ac978325c416347df74141",
  measurementId: "G-HH0B5WNLCF"
};

// Initialize Firebase
const app = getApps().length === 0 ? initializeApp(firebaseConfig) : getApps()[0];
const analytics = isSupported().then((yes) => (yes ? getAnalytics(app) : null));

export { app, analytics };
